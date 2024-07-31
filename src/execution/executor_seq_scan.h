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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;

        // if(context)
        // {
        //     context->lock_mgr_->lock_shared_on_table(context->txn_, fh_->GetFd());
        // }
    }
    
    bool is_end() const override { return scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    RmFileHandle* get_fh() const {
        return fh_;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        //构建表迭代器scan_
        scan_ = std::make_unique<RmScan>(fh_);//此时指向第一个存放记录的位置

        //开始迭代扫描
        for (; !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (eval_conds(cols_, fed_conds_, rec.get())) {
                    break;
                }
            } catch (RecordNotFoundError &e) {
                std::cerr << e.what() << std::endl;
            }
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        for(scan_->next(); !scan_->is_end(); scan_->next())
        {
            rid_ = scan_->rid();//得到元组
            //扫描到第一个谓词条件的元组停止
            if(eval_conds(cols_, fed_conds_, fh_->get_record(rid_, context_).get()))
                break;
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return fh_->get_record(rid_, context_);
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
};