/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (is_dir(db_name))//该数据库存在
    {
        if(chdir(db_name.c_str()) < 0)
        {
            throw UnixError();
        }
        std::ifstream ifs(DB_META_NAME);//打开数据库元数据文件
        ifs >> db_;//加载数据库元数据
        ifs.close();

        //打开所有的表文件和索引文件，并将文件句柄加载到系统中
        for(auto& entry: db_.tabs_)
        {
            auto& tab = entry.second;//数据库中的每张表
            //打开表文件
            fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));

            //打开表文件上的索引文件
            for(auto index: tab.indexes)
            {
                ihs_.emplace(ix_manager_->get_index_name(tab.name, index.cols), 
                ix_manager_->open_index(tab.name, index.cols));
            }
        }
    }
    else//该数据库不存在
    {
        throw DatabaseNotFoundError(db_name);
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    std::ofstream ofs(DB_META_NAME);
    //将获取的DBMeta成员变量将meta数据Dump到文件中
    ofs << db_;

    //清理SmManager.db_
    db_.name_.clear();
    db_.tabs_.clear();

    //关闭数据库表的记录文件和索引文件
    for(auto& entry: fhs_)
    {
        rm_manager_->close_file(entry.second.get());
    }
    fhs_.clear();

    for(auto& entry: ihs_)
    {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();

    if (chdir("..") < 0) 
    {
        throw UnixError();
    }

    flush_meta();
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    // if(context)
    // {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // if (context) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    //获取表元数据TabMeta
    TabMeta &tab = db_.get_table(tab_name);

    //删除记录文件
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);

    //删除索引文件
    for (auto &tab_index: tab.indexes)
    {
        drop_index(tab_name, tab_index.cols, context);
    }

    db_.tabs_.erase(tab_name); 
    fhs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {

    auto& tab_meta = db_.get_table(tab_name);
    IndexMeta index_meta = {tab_name};
    std::vector<ColMeta> &col_meta = index_meta.cols;
    for (auto& col : col_names) {
        auto it = tab_meta.get_col(col);
        col_meta.push_back(*it);
        index_meta.col_tot_len += it->len;
        index_meta.col_num ++ ;
    }
    if (context && !context->lock_mgr_->lock_exclusive_on_table(context->txn_, disk_manager_->get_fd2path(tab_name)))
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    ix_manager_->create_index(tab_name, col_meta);
    tab_meta.indexes.push_back(index_meta);
    ihs_[ix_manager_->get_index_name(tab_name, col_meta)] = ix_manager_->open_index(tab_name, col_meta);
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    //获得索引文件的名字
    std::string ix_index_name = ix_manager_->get_index_name(tab_name, col_names);

    //将索引文件关闭
    ix_manager_->close_index(ihs_[ix_index_name].get());

    //将索引文件删除
    ix_manager_->destroy_index(tab_name, col_names);

    //获取表元数据
    TabMeta &tab = db_.get_table(tab_name);
    //更新tab的indexes
    tab.indexes.erase(tab.get_index_meta(col_names));
    //更新ihs_
    ihs_.erase(ix_index_name);
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    //获得索引包含的字段元数据的名称
    std::vector<std::string> col_names;
    for(auto &col: cols)
    {
        col_names.push_back(col.name);
    }

    //调用上一个drop_index函数
    drop_index(tab_name, col_names, context);

}