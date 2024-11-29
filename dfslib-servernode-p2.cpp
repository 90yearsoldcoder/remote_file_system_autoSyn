#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;

using dfs_service::compareRes;
using dfs_service::DFSService;
using dfs_service::FileChunk;
using dfs_service::FileCRC;
using dfs_service::FileInfo;
using dfs_service::FilePath;
using dfs_service::FileStatus;
using dfs_service::ListFilesRequest;
using dfs_service::LockerInfo;
using dfs_service::LSResponse;
using dfs_service::ResponseStatus;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = dfs_service::ListFilesRequest;
using FileListResponseType = dfs_service::LSResponse;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final : public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
                             public DFSCallDataManager<FileRequestType, FileListResponseType>
{

private:
    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath)
    {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

    /** file lockers **/
    std::unordered_map<std::string, std::string> fileOwners;

    /** file modification flag **/
    modificationInfo modInfo;

    void NotifyModification(std::string reason)
    {
        std::lock_guard<std::mutex> lock(modInfo.mtx);
        dfs_log(LL_DEBUG) << "Modification detected: " << reason;
        modInfo.modifictionFlag = true;
        modInfo.cv.notify_all(); // Signal all waiting threads
    }

public:
    DFSServiceImpl(const std::string &mount_path, const std::string &server_address, int num_async_threads) : mount_path(mount_path), crc_table(CRC::CRC_32())
    {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]
                                               { this->ProcessQueuedRequests(); });
    }

    ~DFSServiceImpl()
    {
        this->runner.Shutdown();
    }

    void Run()
    {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext *context,
                         FileRequestType *request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType> *response,
                         grpc::ServerCompletionQueue *cq,
                         void *tag)
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        dfs_log(LL_DEBUG3) << "push a new request to the queue";
        this->queued_tags.emplace_back(context, request, response, cq, tag);
    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext *context, FileRequestType *request, FileListResponseType *response)
    {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        // Open the directory
        DIR *dir = opendir(mount_path.c_str());

        // Read the directory
        struct dirent *entry;
        struct stat file_stat;
        while ((entry = readdir(dir)) != nullptr)
        {
            const std::string filename = entry->d_name;
            const std::string wrapped_path = WrapPath(filename);
            if (stat(wrapped_path.c_str(), &file_stat) != 0)
            {
                dfs_log(LL_ERROR) << "Failed to stat file: " << wrapped_path;
                continue;
            }

            // record the file info
            dfs_service::FileInfo *file_info = response->add_filesinfolist();
            file_info->set_filename(filename);
            file_info->set_modified_time(file_stat.st_mtime);
            file_info->set_crc(dfs_file_checksum(wrapped_path, &this->crc_table));
        }

        closedir(dir);
        dfs_log(LL_SYSINFO) << "Thread " << std::this_thread::get_id() << " : " << "Sent file lists successfully";
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests()
    {
        while (true)
        {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //

            // check the modification flag
            std::unique_lock<std::mutex> lock(modInfo.mtx);
            modInfo.cv.wait(lock, [&]
                            { return modInfo.modifictionFlag; });

            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);

                for (QueueRequest<FileRequestType, FileListResponseType> &queue_request : this->queued_tags)
                {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                                              queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                                            this->queued_tags.begin(),
                                            this->queued_tags.end(),
                                            [](QueueRequest<FileRequestType, FileListResponseType> &queue_request)
                                            { return queue_request.finished; }),
                                        this->queued_tags.end());
            }

            // reset the modification flag
            modInfo.modifictionFlag = false;
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //

    ::grpc::Status requestLock(::grpc::ServerContext *context,
                               const ::dfs_service::LockerInfo *request,
                               ::dfs_service::ResponseStatus *response) override
    {
        std::string path = request->path();
        std::string clientid = request->clientid();

        if (context->IsCancelled())
        {
            return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
        }

        if (fileOwners.find(path) == fileOwners.end())
        {
            fileOwners[path] = clientid;
            response->set_descstatus("OK");
            dfs_log(LL_DEBUG) << "Lock granted for " << path << " to " << clientid;
            return grpc::Status(StatusCode::OK, "Lock granted");
        }
        else if (fileOwners[path] == clientid)
        {
            response->set_descstatus("OK");
            dfs_log(LL_DEBUG) << "Lock granted for " << path << " to " << clientid;
            return grpc::Status(StatusCode::OK, "Lock granted");
        }
        else
        {
            response->set_descstatus("RESOURCE_EXHAUSTED");
            dfs_log(LL_DEBUG) << "Cannot grant lock for " << path << " to " << clientid;
            return grpc::Status(StatusCode::RESOURCE_EXHAUSTED, "Resource exhausted");
        }
    }

    ::grpc::Status releaseLock(::grpc::ServerContext *context,
                               const ::dfs_service::LockerInfo *request,
                               ::dfs_service::ResponseStatus *response) override
    {
        std::string path = request->path();
        std::string clientid = request->clientid();

        if (context->IsCancelled())
        {
            return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
        }

        if (fileOwners.find(path) != fileOwners.end())
        {
            if (fileOwners[path] == clientid)
            {
                fileOwners.erase(path);
                response->set_descstatus("OK");
                dfs_log(LL_DEBUG) << "Lock released for " << path << " by " << clientid;
                return grpc::Status(StatusCode::OK, "Lock released");
            }
            else
            {
                response->set_descstatus("Not the owner");
                return grpc::Status(StatusCode::OK, "Not the owner");
            }
        }
        else
        {
            response->set_descstatus("NOT_FOUND");
            return grpc::Status(StatusCode::OK, "Lock not found");
        }
    }

    ::grpc::Status compareFile(::grpc::ServerContext *context,
                               const ::dfs_service::FileCRC *request,
                               ::dfs_service::compareRes *response) override
    {
        std::string fileName = request->filename();
        std::uint32_t localCRC = request->crc();
        std::uint32_t serverCRC = dfs_file_checksum(WrapPath(fileName), &this->crc_table);

        if (context->IsCancelled())
        {
            return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
        }

        if (localCRC == serverCRC)
        {
            response->set_same(true);
        }
        else
        {
            dfs_log(LL_DEBUG) << "crc for local file: " << localCRC << " crc for server file: " << serverCRC;
            response->set_same(false);
        }

        dfs_log(LL_DEBUG) << "CRC comparison for " << fileName << " : " << (response->same() ? "Same" : "Different");
        return grpc::Status(StatusCode::OK, "CRC comparison done");
    }

    ::grpc::Status storeFile(::grpc::ServerContext *context,
                             ::grpc::ServerReader<::dfs_service::FileChunk> *reader,
                             ::dfs_service::ResponseStatus *response) override
    {
        // get the filename from the metadata
        const auto &metadata = context->client_metadata();
        auto iter = metadata.find("filename");
        // get the client id from the metadata
        auto iter2 = metadata.find("clientid");

        if (iter == metadata.end())
        {
            dfs_log(LL_ERROR) << "Filename not found in metadata";
            return grpc::Status(StatusCode::CANCELLED, "Filename not found in metadata");
        }

        if (iter2 == metadata.end())
        {
            dfs_log(LL_ERROR) << "Client ID not found in metadata";
            return grpc::Status(StatusCode::CANCELLED, "Client ID not found in metadata");
        }

        std::string filename = std::string(iter->second.data(), iter->second.size());
        std::string clientid = std::string(iter2->second.data(), iter2->second.size());
        // wrap the path
        std::string filepath = WrapPath(filename);

        // check if the client has the write lock(This is for double-check)
        if (fileOwners.find(filename) == fileOwners.end() || fileOwners[filename] != clientid)
        {
            dfs_log(LL_ERROR) << "Client does not have the write lock for the file";
            return grpc::Status(StatusCode::RESOURCE_EXHAUSTED, "Client does not have the write lock for the file");
        }

        // open the file to writie the chunks
        std::ofstream outfile(filepath, std::ios::out | std::ios::binary);
        if (!outfile.is_open())
        {
            dfs_log(LL_ERROR) << "Failed to open file for writing: " << filepath;
            return grpc::Status(StatusCode::INTERNAL, "Failed to open file for writing");
        }

        dfs_service::FileChunk chunk;
        int64_t bytes_written = 0;
        while (reader->Read(&chunk))
        {
            const std::string &content = chunk.content();
            outfile.write(content.data(), content.size());
            bytes_written += content.size();

            // Check for client cancellation
            if (context->IsCancelled())
            {
                dfs_log(LL_SYSINFO) << "Client cancelled the request.";
                outfile.close();
                return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
            }

            // For debugging purposes
            dfs_log(LL_DEBUG) << "Writing " << chunk.chunk_num() << " chunk: " << content.size() << " bytes";
        }
        outfile.close();

        response->set_descstatus("File stored successfully");
        NotifyModification("Store");

        return grpc::Status(StatusCode::OK, "File stored successfully");
    }

    ::grpc::Status fetchFile(::grpc::ServerContext *context,
                             const ::dfs_service::FilePath *request,
                             ::grpc::ServerWriter<::dfs_service::FileChunk> *writer) override
    {
        std::string wrapedPath = WrapPath(request->path());
        std::ifstream infile(wrapedPath, std::ios::in | std::ios::binary);
        // check if the file exists
        if (!infile.is_open())
        {
            dfs_log(LL_ERROR) << "File not found: " << wrapedPath;
            return grpc::Status(StatusCode::NOT_FOUND, "File not found");
        }

        // prepare the buffer to read the file
        char buffer[BUF_SIZE];
        int32_t chunk_num = 0;
        while (infile.read(buffer, BUF_SIZE) || infile.gcount())
        {
            dfs_service::FileChunk chunk;
            chunk.set_content(buffer, infile.gcount());
            chunk.set_chunk_num(chunk_num++);
            if (context->IsCancelled())
            {
                dfs_log(LL_SYSINFO) << "Client cancelled the request.";
                infile.close();
                return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
            }
            if (!writer->Write(chunk))
            {
                dfs_log(LL_ERROR) << "Failed to write chunk to stream(from server to clinet)";
                infile.close();
                return grpc::Status(StatusCode::CANCELLED, "Failed to write chunk to client");
            }
            dfs_log(LL_DEBUG) << "Writing chunk: " << chunk_num << " size: " << infile.gcount();
        }
        infile.close();

        return grpc::Status(StatusCode::OK, "File sent successfully");
    }

    ::grpc::Status deleteFile(::grpc::ServerContext *context,
                              const ::dfs_service::FilePath *request,
                              ::dfs_service::ResponseStatus *response) override
    {
        // check if the context is cancelled
        if (context->IsCancelled())
        {
            dfs_log(LL_SYSINFO) << "Client cancelled the request.";
            return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
        }

        std::string filename = request->path();
        std::string path = WrapPath(filename);
        // check if the file exists
        struct stat file_stat;
        if (stat(path.c_str(), &file_stat) != 0)
        {
            dfs_log(LL_ERROR) << "File not found: " << path;
            return grpc::Status(StatusCode::NOT_FOUND, "File not found");
        }

        // get the client id from the metadata
        const auto &metadata = context->client_metadata();
        auto iter2 = metadata.find("clientid");
        if (iter2 == metadata.end())
        {
            dfs_log(LL_ERROR) << "Client ID not found in metadata";
            return grpc::Status(StatusCode::CANCELLED, "Client ID not found in metadata");
        }
        // double check the client has the write lock
        std::string clientid = std::string(iter2->second.data(), iter2->second.size());
        // check if the client has the write lock(This is for double-check)
        if (fileOwners.find(filename) == fileOwners.end() || fileOwners[filename] != clientid)
        {
            dfs_log(LL_ERROR) << "Client does not have the write lock for the file";
            return grpc::Status(StatusCode::RESOURCE_EXHAUSTED, "Client does not have the write lock for the file");
        }

        // remove the file
        if (std::remove(path.c_str()) != 0)
        {
            dfs_log(LL_ERROR) << "Failed to delete file: " << path;
            return grpc::Status(StatusCode::CANCELLED, "Failed to delete file");
        }

        dfs_log(LL_SYSINFO) << "File deleted successfully";
        NotifyModification("Delete");
        return grpc::Status::OK;
    }

    ::grpc::Status listFiles(::grpc::ServerContext *context,
                             const ::dfs_service::ListFilesRequest *request,
                             ::dfs_service::LSResponse *response) override
    {
        // Open the directory
        DIR *dir = opendir(mount_path.c_str());
        if (dir == nullptr)
        {
            dfs_log(LL_ERROR) << "Failed to open directory: " << strerror(errno);
            return grpc::Status(grpc::INTERNAL, "Failed to open directory.");
        }

        // Read the directory
        struct dirent *entry;
        struct stat file_stat;
        while ((entry = readdir(dir)) != nullptr)
        {
            if (context->IsCancelled())
            {
                dfs_log(LL_SYSINFO) << "Client cancelled the request.";
                closedir(dir);
                return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
            }

            const std::string filename = entry->d_name;
            const std::string wrapped_path = WrapPath(filename);
            if (stat(wrapped_path.c_str(), &file_stat) != 0)
            {
                dfs_log(LL_ERROR) << "Failed to stat file: " << wrapped_path;
                continue;
            }

            // record the file info
            dfs_service::FileInfo *file_info = response->add_filesinfolist();
            file_info->set_filename(filename);
            file_info->set_modified_time(file_stat.st_mtime);
        }

        closedir(dir);
        dfs_log(LL_SYSINFO) << "Sent file lists successfully";

        return grpc::Status::OK;
    }

    ::grpc::Status statusFile(::grpc::ServerContext *context,
                              const ::dfs_service::FilePath *request,
                              ::dfs_service::FileStatus *response) override
    {
        std::string path = WrapPath(request->path());
        struct stat file_stat;

        if (context->IsCancelled())
        {
            dfs_log(LL_SYSINFO) << "Client cancelled the request.";
            return grpc::Status(StatusCode::DEADLINE_EXCEEDED, "Client cancelled the request.");
        }

        if (stat(path.c_str(), &file_stat) != 0)
        {
            dfs_log(LL_ERROR) << "Failed to stat file: " << path;
            return grpc::Status(StatusCode::NOT_FOUND, "File not found");
        }

        response->set_size(file_stat.st_size);
        response->set_modified_time(file_stat.st_mtime);
        response->set_creation_time(file_stat.st_ctime);
        dfs_log(LL_DEBUG) << "File " << path << " size: " << file_stat.st_size << " mtime: " << file_stat.st_mtime << " ctime: " << file_stat.st_ctime;
        dfs_log(LL_SYSINFO) << "File status retrieved successfully";

        return grpc::Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
                             const std::string &mount_path,
                             int num_async_threads,
                             std::function<void()> callback) : server_address(server_address),
                                                               mount_path(mount_path),
                                                               num_async_threads(num_async_threads),
                                                               grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept
{
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start()
{
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);

    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
