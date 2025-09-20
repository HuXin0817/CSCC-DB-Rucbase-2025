#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const
{
    // 1. 获取指定记录所在的page handle
    auto page_handle = fetch_page_handle(rid.page_no);

    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    char *record_data = page_handle.get_slot(rid.slot_no);
    return std::make_unique<RmRecord>(record_data, file_hdr_.record_size);
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char *buf, Context *context)
{
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    // 1. 获取当前未满的page handle

    RmPageHandle page_handle{};
    if (file_hdr_.first_free_page_no == INVALID_PAGE_ID)
    {
        page_handle = create_new_page_handle();
    }
    else
    {
        page_handle = fetch_page_handle(file_hdr_.first_free_page_no);
    }

    // 2. 在page handle中找到空闲slot位置
    int slot_no = Bitmap::find_first_zero(page_handle.bitmap, file_hdr_.num_records_per_page);

    Rid rid_ = Rid{.page_no = page_handle.page->id_.page_no, .slot_no = slot_no};

    if (context != nullptr)
    {
        RmRecord record_(file_hdr_.record_size, buf);
        if (context->txn_ != nullptr)
        {
            WriteRecord write_record_(WriteType::INSERT_TUPLE, tab_name_, rid_, record_);
            context->txn_->append_write_record(write_record_);
            context->log_mgr_->add_insert_log_to_buffer(context->txn_->get_transaction_id(), record_, rid_, tab_name_);
        }
    }

    // 3. 将buf复制到空闲slot位置
    std::memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);

    // 4. 更新page_handle.page_hdr中的数据结构
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;

    // 更新file_hdr_.first_free_page_no
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
    {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = INVALID_PAGE_ID;
    }

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    return rid_;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::reset_data_on_rid(const Rid &rid, char *buf)
{
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!is_record(rid))
    {
        page_handle.page_hdr->num_records++;
    }
    std::memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context)
{
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    // 1. 获取指定记录所在的page handle

    RmPageHandle page_handle{};
    try
    {
        page_handle = fetch_page_handle(rid.page_no);
    }
    catch (const RecordNotFoundError &)
    {
        return;
    }

    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no))
    {
        Bitmap::reset(page_handle.bitmap, rid.slot_no);
        return;
    }

    if (context != nullptr)
    {
        auto record_ = get_record(rid);
        if (context->txn_ != nullptr)
        {
            WriteRecord write_record_(WriteType::DELETE_TUPLE, tab_name_, rid, *record_);
            context->txn_->append_write_record(write_record_);
            // 日志管理
            context->log_mgr_->add_delete_log_to_buffer(context->txn_->get_transaction_id(), *record_, rid, tab_name_);
        }
    }

    // 2. 更新page_handle.page_hdr中的数据结构
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;

    // 处理页面变成未满的情况
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1)
    {
        release_page_handle(page_handle);
    }

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context)
{
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    if (context != nullptr)
    {
        auto record_ = get_record(rid);
        WriteRecord write_record(WriteType::UPDATE_TUPLE, tab_name_, rid, *record_);
        if (context->txn_ != nullptr)
        {
            context->txn_->append_write_record(write_record);

            RmRecord update_record(file_hdr_.record_size);
            memcpy(update_record.data, buf, update_record.size);
            context->log_mgr_->add_update_log_to_buffer(context->txn_->get_transaction_id(), update_record, *record_, rid, tab_name_);
        }
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    }

    std::memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const
{
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no < 0)
    {
        throw RecordNotFoundError(page_no, -1);
    }

    if (page_no >= file_hdr_.num_pages)
    {
        throw RecordNotFoundError(page_no, -1);
    }

    Page *page = buffer_pool_manager_->fetch_page({fd_, page_no});
    if (!page)
    {
        throw RecordNotFoundError(page_no, -1);
    }

    return {&file_hdr_, page};
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle()
{
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    PageId new_page_id = {fd_, file_hdr_.num_pages};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    if (!page)
    {
        throw std::runtime_error("Failed to create new page.");
    }

    RmPageHandle page_handle(&file_hdr_, page);
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = new_page_id.page_no;
    file_hdr_.num_pages++;

    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle()
{
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if (file_hdr_.first_free_page_no == INVALID_PAGE_ID)
    {
        return create_new_page_handle();
    }

    return fetch_page_handle(file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle)
{
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    if (page_handle.page_hdr->num_records < file_hdr_.num_records_per_page)
    {
        page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
        file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
    }
}

size_t RmFileHandle::record_size() const { return file_hdr_.record_size; }
