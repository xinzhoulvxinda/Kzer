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
 * @description: 鍒ゆ柇鏄惁涓轰竴涓枃浠跺す
 * @return {bool} 杩斿洖鏄惁涓轰竴涓枃浠跺す
 * @param {string&} db_name 鏁版嵁搴撴枃浠跺悕绉帮紝涓庢枃浠跺す鍚屽悕
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 鍒涘缓鏁版嵁搴擄紝鎵€鏈夌殑鏁版嵁搴撶浉鍏虫枃浠堕兘鏀惧湪鏁版嵁搴撳悓鍚嶆枃浠跺す涓?
 * @param {string&} db_name 鏁版嵁搴撳悕绉?
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 涓烘暟鎹簱鍒涘缓涓€涓瓙鐩綍
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 鍒涘缓涓€涓悕涓篸b_name鐨勭洰褰?
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 杩涘叆鍚嶄负db_name鐨勭洰褰?
        throw UnixError();
    }
    // 鍒涘缓绯荤粺鐩綍
    DbMeta* new_db = new DbMeta();
    new_db->name_ = db_name;

    // 娉ㄦ剰锛屾澶刼fstream浼氬湪褰撳墠鐩綍鍒涘缓(濡傛灉娌℃湁姝ゆ枃浠跺厛鍒涘缓)鍜屾墦寮€涓€涓悕涓篋B_META_NAME鐨勬枃浠?
    std::ofstream ofs(DB_META_NAME);

    // 灏唍ew_db涓殑淇℃伅锛屾寜鐓у畾涔夊ソ鐨刼perator<<鎿嶄綔绗︼紝鍐欏叆鍒皁fs鎵撳紑鐨凞B_META_NAME鏂囦欢涓?
    ofs << *new_db;  // 娉ㄦ剰锛氭澶勯噸杞戒簡鎿嶄綔绗?<

    delete new_db;

    // 鍒涘缓鏃ュ織鏂囦欢
    disk_manager_->create_file(LOG_FILE_NAME);

    // 鍥炲埌鏍圭洰褰?
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 鍒犻櫎鏁版嵁搴擄紝鍚屾椂闇€瑕佹竻绌虹浉鍏虫枃浠朵互鍙婃暟鎹簱鍚屽悕鏂囦欢澶?
 * @param {string&} db_name 鏁版嵁搴撳悕绉帮紝涓庢枃浠跺す鍚屽悕
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
 * @description: 鎵撳紑鏁版嵁搴擄紝鎵惧埌鏁版嵁搴撳搴旂殑鏂囦欢澶癸紝骞跺姞杞芥暟鎹簱鍏冩暟鎹拰鐩稿叧鏂囦欢
 * @param {string&} db_name 鏁版嵁搴撳悕绉帮紝涓庢枃浠跺す鍚屽悕
 */
void SmManager::open_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        if (chdir(db_name.c_str()) < 0) {
            throw UnixError();
        }
        std::ifstream ifs(DB_META_NAME);
        ifs >> db_;
        ifs.close();
        for (auto& entry : db_.tabs_) {
            auto& tab = entry.second;
            fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));
            for (auto index : tab.indexes) {
                ihs_.emplace(ix_manager_->get_index_name(tab.name, index.cols),
                             ix_manager_->open_index(tab.name, index.cols));
            }
            for (auto index : tab.indexes) {
                drop_index(tab.name, index.cols, nullptr);
            }
        }
    } else {
        throw DatabaseNotFoundError(db_name);
    }
}

/**
 * @description: 鎶婃暟鎹簱鐩稿叧鐨勫厓鏁版嵁鍒峰叆纾佺洏涓?
 */
void SmManager::flush_meta() {
    // 榛樿娓呯┖鏂囦欢
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 鍏抽棴鏁版嵁搴撳苟鎶婃暟鎹惤鐩?
 */
void SmManager::close_db() {
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
    db_.name_.clear();
    db_.tabs_.clear();

    for (auto& entry : fhs_) rm_manager_->close_file(entry.second.get());
    for (auto& entry : ihs_) ix_manager_->close_index(entry.second.get());

    fhs_.clear();
    ihs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
    flush_meta();
}

/**
 * @description: 鏄剧ず鎵€鏈夌殑琛?閫氳繃娴嬭瘯闇€瑕佸皢鍏剁粨鏋滃啓鍏ュ埌output.txt,璇︽儏鐪嬮鐩枃妗?
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
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 鏄剧ず琛ㄧ殑鍏冩暟鎹?
 * @param {string&} tab_name 琛ㄥ悕绉?
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 鍒涘缓琛?
 * @param {string&} tab_name 琛ㄧ殑鍚嶇О
 * @param {vector<ColDef>&} col_defs 琛ㄧ殑瀛楁
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
    for (auto& col_def : col_defs) {
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
    int record_size = curr_offset;  // record_size灏辨槸col meta鎵€鍗犵殑澶у皬锛堣〃鐨勫厓鏁版嵁涔熸槸浠ヨ褰曠殑褰㈠紡杩涜瀛樺偍鐨勶級
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    // // 鐢宠琛ㄧ骇鍐欓攣
    // if (context) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    flush_meta();
}

/**
 * @description: 鍒犻櫎琛?
 * @param {string&} tab_name 琛ㄧ殑鍚嶇О
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // // 鐢宠琛ㄧ骇鍐欓攣
    // if (context) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    TabMeta& tab = db_.get_table(tab_name);
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);
    for (IndexMeta& index_meta : tab.indexes) {
        drop_index(tab_name, index_meta.cols, context);
    }
    db_.tabs_.erase(tab_name);
    fhs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 鍒涘缓绱㈠紩
 * @param {string&} tab_name 琛ㄧ殑鍚嶇О
 * @param {vector<string>&} col_names 绱㈠紩鍖呭惈鐨勫瓧娈靛悕绉?
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {  
    if (!db_.is_table(tab_name)) {  
        throw TableNotFoundError(tab_name);  
    }  
  
    TabMeta& tab = db_.get_table(tab_name);  
  
    std::vector<ColMeta*> col_ptrs;  
    std::vector<ColMeta> cols;  
    int col_tot_len = 0;  
    for (const auto& col_name : col_names) {  
        bool found = false;  
        for (auto& col : tab.cols) {  
            if (col.name == col_name) {  
                if (col.index) {  
                    throw IndexExistsError(tab_name, col_names);  
                }  
                col_ptrs.push_back(&col);  
                cols.push_back(col);  
                col_tot_len += col.len;  
                found = true;  
                break;  
            }  
        }  
        if (!found) {  
            throw ColumnNotFoundError(col_name);  
        }  
    }  
  
    IndexMeta idx_meta;  
    idx_meta.tab_name = tab_name;  
    idx_meta.cols = cols;  
    idx_meta.col_tot_len = col_tot_len;  
    idx_meta.col_num = cols.size();  
  
    ix_manager_->create_index(tab_name, cols);  
  
    for (auto col_ptr : col_ptrs) {  
        col_ptr->index = true;  
    }  
  
    tab.indexes.push_back(idx_meta);  
  
    flush_meta();  
}


/**
 * @description: 鍒犻櫎绱㈠紩
 * @param {string&} tab_name 琛ㄥ悕绉?
 * @param {vector<string>&} col_names 绱㈠紩鍖呭惈鐨勫瓧娈靛悕绉?
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 鐢宠琛ㄧ骇璇婚攣
    // context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());

    if (!ix_manager_->exists(tab_name, col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);

    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, col_names);

    TabMeta& tab = db_.get_table(tab_name);
    tab.indexes.erase(tab.get_index_meta(col_names));

    ihs_.erase(index_name);
    flush_meta();
}

/**
 * @description: 鍒犻櫎绱㈠紩
 * @param {string&} tab_name 琛ㄥ悕绉?
 * @param {vector<ColMeta>&} 绱㈠紩鍖呭惈鐨勫瓧娈靛厓鏁版嵁
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    for (auto& col : cols) {
        col_names.push_back(col.name);
    }
    drop_index(tab_name, col_names, context);
}