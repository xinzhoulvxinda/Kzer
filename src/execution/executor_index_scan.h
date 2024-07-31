/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;

        if(context)
        {
            context->lock_mgr_->lock_shared_on_table(context->txn_, fh_->GetFd());
        }
    }

    void beginTuple() override {
        // auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        // auto lower_leaf = ih->leaf_begin();
        // auto upper_leaf = ih->leaf_end();
        
        // //如果条件是在建立索引的列上，那么可以采用区间查询
        // for(auto &cond : fed_conds_)
        // {
        //     if(index_established(cond.lhs_col))
        // }
    }

    void nextTuple() override {
        
    }

    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    Rid &rid() override { return rid_; }

    /**
    * @description: 判断元组是否满足单个谓词条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols scan后生成的记录的字段
    * @param {Condition &} cond 谓词条件
    * @param {RmRecord *} rec scan后生成的记录
    */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec)
    {
        //需判断左侧字段是否有效和右侧值是否有效，根据测试文件得出


        //找到谓词条件中的那个字段（get_col函数可以检查左侧字段是否有效）
        auto lhs_col = get_col(rec_cols, cond.lhs_col);

        //得到该字段对应的值
        char *lhs = rec->data + lhs_col->offset;

        //条件右侧的类型
        ColType rhs_type;
        //条件右侧的值
        char *rhs;

        //判断条件右侧
        if(cond.is_rhs_val)//如果条件右端是值
        {
            rhs_type = cond.rhs_val.type;//条件右侧的类型
            rhs = cond.rhs_val.raw->data;//条件右侧的值
        }
        else//如果条件右端是列名
        {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = cond.rhs_val.raw->data;//此处可能有问题
        }

        //判断左侧字段是否满足条件
        int result = ix_compare(lhs, rhs, rhs_type, lhs_col->len);//比较左侧和右侧值的大小
        if(cond.op == OP_EQ)
            return result == 0;
        else if(cond.op == OP_NE)
            return result != 0;
        else if(cond.op == OP_LT)
            return result < 0;
        else if(cond.op == OP_GT)
            return result > 0;
        else if(cond.op == OP_LE)
            return result <= 0;
        else if (cond.op == OP_GE)
            return result >= 0;

    }

    /**
    * @description: 判断元组是否满足所有谓词条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols scan后生成的记录的字段
    * @param {std::vector<Condition> &} conds 谓词条件
    * @param {RmRecord *} rec scan后生成的记录
    */
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec)
    {
        for(auto &cond: conds)
        {
            if(eval_cond(rec_cols, cond, rec))
                continue;
            else
                return false;
        }
        return true;
    }

    bool index_established(TabCol target)
    {
        return get_col(tab_.cols, target)->index;
    }
};