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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    bool is_end() const override { return left_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        while(!is_end())
        {
            if(eval_conds(cols_, fed_conds_, left_->Next().get(), right_->Next().get()))
                break;
            right_->nextTuple();
            if(right_->is_end())
            {
                left_->nextTuple();
                right_->beginTuple();
            }
        }

    }

    void nextTuple() override {
        right_->nextTuple();
        if(right_->is_end())
        {
            left_->nextTuple();
            right_->beginTuple();
        }
        while(!is_end())
        {
            if(eval_conds(cols_, fed_conds_, left_->Next().get(), right_->Next().get()))
                break;
            right_->nextTuple();
            if(right_->is_end())
            {
                left_->nextTuple();
                right_->beginTuple();
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        auto record = std::make_unique<RmRecord>(len_);
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();

        memcpy(record->data, left_rec->data, left_rec->size);
        memcpy(record->data + left_rec->size, right_rec->data, right_rec->size);
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    /**
    * @description: 判断左面的元组和右面的元组是否满足连接条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols 连接后的元组的字段
    * @param {Condition &} cond 谓词条件
    * @param {RmRecord *} lrec 左元组的记录
    * @param {RmRecord *} rrec 右元组的记录
    */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *lrec, const RmRecord *rrec)
    {
        //找到连接条件中左侧的字段（get_col函数可以检查左侧字段是否有效）
        auto lhs_col = get_col(rec_cols, cond.lhs_col);

        //得到该字段对应的值
        char *lhs = lrec->data + lhs_col->offset;

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
            rhs = rrec->data + rhs_col->offset - left_->tupleLen();//条件右端列的值
        }

        //判断是否满足连接条件
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
    * @param {std::vector<ColMeta> &} rec_cols 连接后的元组的字段
    * @param {Condition &} cond 谓词条件
    * @param {RmRecord *} lrec 左元组的记录
    * @param {RmRecord *} rrec 右元组的记录
    */
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *lrec, const RmRecord *rrec)
    {
        for(auto &cond: conds)
        {
            if(eval_cond(rec_cols, cond, lrec, rrec))
                continue;
            else
                return false;
        }
        return true;
    }
};