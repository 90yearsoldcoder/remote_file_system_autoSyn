#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
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

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = dfs_service::ListFilesRequest;
using FileListResponseType = dfs_service::LSResponse;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    // Create the request
    dfs_service::LockerInfo lockerinfo;
    lockerinfo.set_path(filename);
    lockerinfo.set_clientid(client_id);
    // Create the response
    dfs_service::ResponseStatus response;
    // Create the context
    grpc::ClientContext context;

    // Call the service
    dfs_log(LL_DEBUG2) << "Requesting write access for " << filename;
    grpc::Status status = service_stub->requestLock(&context, lockerinfo, &response);

    // Check the status
    if (!status.ok())
    {
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
        {
            return grpc::StatusCode::DEADLINE_EXCEEDED;
        }
        else if (status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED)
        {
            return grpc::StatusCode::RESOURCE_EXHAUSTED;
        }
        else
        {
            return grpc::StatusCode::CANCELLED;
        }
    }

    dfs_log(LL_DEBUG2) << "Write access granted for " << filename;
    return grpc::StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::ReleaseWriteAccess(const std::string &filename)
{
    /*
     it will send a rpc to server to release the write lock
     OK - if all went well
     StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
     StatusCode::CANCELLED otherwise
    */
    // Create the request
    dfs_service::LockerInfo lockerinfo;
    lockerinfo.set_path(filename);
    lockerinfo.set_clientid(client_id);
    // Create the response
    dfs_service::ResponseStatus response;
    // Create the context
    grpc::ClientContext context;

    // Call the service
    dfs_log(LL_DEBUG2) << "Releasing write access for " << filename;
    grpc::Status status = service_stub->releaseLock(&context, lockerinfo, &response);

    // Check the status
    if (!status.ok())
    {
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
        {
            return grpc::StatusCode::DEADLINE_EXCEEDED;
        }
        else
        {
            return grpc::StatusCode::CANCELLED;
        }
    }

    dfs_log(LL_DEBUG2) << "Write access released for " << filename;
    return grpc::StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::compareServerCRC(const std::string &filename)
{
    /*
    it will send a rpc to server to get the crc of the file
    OK - if all went well
    StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    StatusCode::ALREADY_EXISTS - if the local file is the same as the server file
    StatusCode::CANCELLED otherwise
    */
    // Create the request
    dfs_service::FileCRC filecrc;
    filecrc.set_filename(filename);
    std::uint32_t crc = dfs_file_checksum(WrapPath(filename), &(this->crc_table));
    filecrc.set_crc(crc);
    // Create the response
    dfs_service::compareRes response;
    // Create the context
    grpc::ClientContext context;

    // Call the service
    dfs_log(LL_DEBUG2) << "Comparing CRC for " << filename;
    grpc::Status status = service_stub->compareFile(&context, filecrc, &response);

    // Check the status
    if (!status.ok())
    {
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
        {
            return grpc::StatusCode::DEADLINE_EXCEEDED;
        }
        else
        {
            return grpc::StatusCode::CANCELLED;
        }
    }

    std::string crc_info = response.same() ? "CRC match" : "CRC mismatch";
    dfs_log(LL_DEBUG2) << "CRC comparison result for " << filename << ": " << crc_info;

    // Return OK with additional details
    if (response.same())
    {
        return grpc::StatusCode::ALREADY_EXISTS;
    }
    return grpc::StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_SYSINFO) << "Storing file: " << filename;
    std::string local_filepath = WrapPath(filename);
    // Check if the file exists
    std::ifstream infile(local_filepath, std::ios::in | std::ios::binary);
    if (!infile.is_open())
    {
        dfs_log(LL_ERROR) << "Local File not found: " << local_filepath;
        ReleaseWriteAccess(filename);
        return StatusCode::NOT_FOUND;
    }

    // ask for write lock
    grpc::StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status != grpc::StatusCode::OK)
    {
        return lock_status;
    }

    // compare the crc
    grpc::StatusCode crc_status = compareServerCRC(filename);
    if (crc_status != grpc::StatusCode::OK)
    {
        dfs_log(LL_ERROR) << "Already exists";
        ReleaseWriteAccess(filename);
        return crc_status;
    }

    // Create the context
    grpc::ClientContext context;
    // Set the metadata
    context.AddMetadata("filename", filename);
    // Set the client id into metadata for the double-check in server
    context.AddMetadata("clientid", client_id);
    // Set the deadline
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    // prepare the response
    ResponseStatus response;

    // Create a ClientWriter for the streaming call
    std::unique_ptr<grpc::ClientWriter<dfs_service::FileChunk>> writer(
        service_stub->storeFile(&context, &response));

    // Create a buffer for the file chunk
    char buffer[BUF_SIZE];
    int32_t chunk_num = 0;

    while (infile.read(buffer, BUF_SIZE) || infile.gcount())
    {
        // Create a FileChunk message
        dfs_service::FileChunk chunk;
        chunk.set_content(buffer, infile.gcount());
        chunk.set_chunk_num(chunk_num++);
        // Write the chunk
        if (!writer->Write(chunk))
        {
            dfs_log(LL_ERROR) << "Failed to write chunk to server";
            break;
        }
        dfs_log(LL_DEBUG) << "Sending chunk No. " << chunk_num << " size: " << infile.gcount();
    }

    // Close the writer
    writer->WritesDone();

    grpc::Status status = writer->Finish();
    infile.close();

    if (status.ok())
    {
        dfs_log(LL_SYSINFO) << "File stored successfully";
        ReleaseWriteAccess(filename);
        return StatusCode::OK;
    }

    if (status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        dfs_log(LL_SYSINFO) << "Deadline exceeded";
        ReleaseWriteAccess(filename);
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else
    {
        dfs_log(LL_SYSINFO) << "Failed to store file: " << status.error_message();
        ReleaseWriteAccess(filename);
        return StatusCode::CANCELLED;
    }
}

grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //

    dfs_log(LL_SYSINFO) << "Fetching file: " << filename;
    // fetch do not need write lock

    // compare the crc
    grpc::StatusCode crc_status = compareServerCRC(filename);
    if (crc_status != grpc::StatusCode::OK)
    {
        return crc_status;
    }

    std::string local_filepath = WrapPath(filename);

    // Create the context
    grpc::ClientContext context;
    // Set the deadline
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    // Set the client id into metadata for the double-check in server
    context.AddMetadata("clientid", client_id);
    // prepare request
    FilePath request;
    request.set_path(filename);

    // Start request
    std::unique_ptr<grpc::ClientReader<dfs_service::FileChunk>> reader(service_stub->fetchFile(&context, request));

    // Create a buffer for the file chunk
    dfs_service::FileChunk chunk;
    int64_t bytes_written = 0;
    bool local_file_exists = false;
    std::ofstream outfile;

    try
    {
        while (reader->Read(&chunk))
        {
            if (local_file_exists == false)
            {
                local_file_exists = true;
                // prepare local file for writing
                outfile.open(local_filepath, std::ios::out | std::ios::binary);
                if (!outfile.is_open())
                {
                    dfs_log(LL_ERROR) << "Failed to open file for writing: " << local_filepath;
                    return StatusCode::INTERNAL;
                }
            }
            if (!outfile.is_open())
            {
                dfs_log(LL_ERROR) << "Failed to open file for writing: " << local_filepath;
                return StatusCode::INTERNAL;
            }
            const std::string &content = chunk.content();
            outfile.write(content.data(), content.size());
            bytes_written += content.size();
            dfs_log(LL_DEBUG) << "Receiving No." << chunk.chunk_num() << " chunk: " << content.size() << " bytes";
        }
        outfile.close();
    }
    catch (const std::exception &e)
    {
        dfs_log(LL_ERROR) << "Failed to send chunk to stream(from client to server)";
        outfile.close();
        return StatusCode::CANCELLED;
    }

    grpc::Status status = reader->Finish();
    if (status.ok())
    {
        dfs_log(LL_SYSINFO) << "File received successfully";
        return StatusCode::OK;
    }
    else if (status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        dfs_log(LL_SYSINFO) << "Deadline exceeded";
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else if (status.error_code() == grpc::NOT_FOUND)
    {
        dfs_log(LL_SYSINFO) << "File not found";
        return StatusCode::NOT_FOUND;
    }
    else
    {
        dfs_log(LL_SYSINFO) << "Other Errors: " << status.error_message();
        return StatusCode::CANCELLED;
    }
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_SYSINFO) << "Deleting file: " << filename;
    // ask for write lock
    grpc::StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status != grpc::StatusCode::OK)
    {
        return lock_status;
    }

    // Why do we need to compare the CRC before deleting the file?
    // If the file is deleted(modified) from the server, the client cannot delete the file from the server.
    grpc::StatusCode crc_status = compareServerCRC(filename);
    if (crc_status == grpc::StatusCode::ALREADY_EXISTS)
    {
        dfs_log(LL_ERROR) << "The file has been modified on the server and cannot be deleted.";
        ReleaseWriteAccess(filename);
        return StatusCode::NOT_FOUND;
    }

    // Create the context
    grpc::ClientContext context;
    // Set the deadline
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    // Set the metadata
    context.AddMetadata("clientid", client_id);
    // prepare request
    dfs_service::FilePath request;
    request.set_path(filename);
    // prepare response
    dfs_service::ResponseStatus response;

    // Call the service
    dfs_log(LL_DEBUG) << "Deleting file: " << filename;
    grpc::Status status = service_stub->deleteFile(&context, request, &response);

    if (!status.ok())
    {
        if (status.error_code() == grpc::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_SYSINFO) << "Deadline exceeded";
            ReleaseWriteAccess(filename);
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (status.error_code() == grpc::NOT_FOUND)
        {
            dfs_log(LL_SYSINFO) << "File not found";
            ReleaseWriteAccess(filename);
            return StatusCode::NOT_FOUND;
        }
        else
        {
            dfs_log(LL_SYSINFO) << "Failed to delete file: " << status.error_message();
            ReleaseWriteAccess(filename);
            return StatusCode::CANCELLED;
        }
    }

    dfs_log(LL_SYSINFO) << "File deleted successfully";
    ReleaseWriteAccess(filename);
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string, int> *file_map, bool display)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    // Create the context
    grpc::ClientContext context;
    // Set the deadline
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    // prepare request
    dfs_service::ListFilesRequest request;
    // prepare response
    dfs_service::LSResponse response;

    // Call the service
    grpc::Status status = service_stub->listFiles(&context, request, &response);

    if (!status.ok())
    {
        if (status.error_code() == grpc::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_ERROR) << "Deadline exceeded";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else
        {
            dfs_log(LL_ERROR) << "Failed to list files: " << status.error_message();
            return StatusCode::CANCELLED;
        }
    }

    // fill the file_map
    for (const auto &file_info : response.filesinfolist())
    {
        dfs_log(LL_DEBUG) << "File: " << file_info.filename() << " - " << file_info.modified_time();
        file_map->insert(std::pair<std::string, int>(file_info.filename(), file_info.modified_time()));
    }

    dfs_log(LL_SYSINFO) << "File list retrieved successfully";
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void *file_status)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    // Create the context
    grpc::ClientContext context;
    // Set the deadline
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    // prepare request
    dfs_service::FilePath request;
    request.set_path(filename);
    // prepare response
    dfs_service::FileStatus response;

    // Call the service
    grpc::Status status = service_stub->statusFile(&context, request, &response);

    if (!status.ok())
    {
        if (status.error_code() == grpc::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_ERROR) << "Deadline exceeded";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (status.error_code() == grpc::NOT_FOUND)
        {
            dfs_log(LL_ERROR) << "File not found";
            return StatusCode::NOT_FOUND;
        }
        else
        {
            dfs_log(LL_ERROR) << "Failed to get file status: " << status.error_message();
            return StatusCode::CANCELLED;
        }
    }

    // define file_status as dfs_service::FileStatus
    file_status = &response;

    dfs_log(LL_SYSINFO) << "File " << filename << " size: " << response.size() << " mtime: " << response.modified_time() << " ctime: " << response.creation_time();

    return StatusCode::OK;
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback)
{

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    std::lock_guard<std::mutex> lock(clientNode_locker);
    callback();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList()
{

    void *tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok))
    {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //
            std::lock_guard<std::mutex> lock(clientNode_locker);

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok)
            {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok())
            {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //

                // Get the list of files from the server
                const FileListResponseType &file_list = call_data->reply;
                for (const auto &file_info : file_list.filesinfolist())
                {
                    std::string filename = file_info.filename();
                    uint64_t modified_time = file_info.modified_time();
                    int32_t crc = file_info.crc();

                    dfs_log(LL_DEBUG2) << "File: " << filename << " - " << modified_time << " crc: " << crc;

                    syncFiles(filename, modified_time, crc);
                }
                // For debug purposes, wait for a few seconds
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            else
            {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here
        }

        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();
    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList()
{
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//

void DFSClientNodeP2::syncFiles(std::string filename, uint64_t modified_time, int32_t crc)
{
    std::string local_filepath = WrapPath(filename);

    // check the file crc with local file
    int32_t local_crc = dfs_file_checksum(local_filepath, &(this->crc_table));

    if (local_crc == crc)
    {
        dfs_log(LL_DEBUG3) << "Local file " << filename << " is the same as the server file";
        return;
    }

    // check the file mtime with local file
    struct stat file_stat;
    if (stat(local_filepath.c_str(), &file_stat) != 0)
    {
        dfs_log(LL_DEBUG3) << "Local file " << filename << " not found";
        Fetch(filename);
        return;
    }
    uint64_t local_file_mtime = file_stat.st_mtime;

    if (local_file_mtime >= modified_time)
    {
        dfs_log(LL_DEBUG3) << "Local file " << filename << " is newer than the server file";
        Store(filename);
        return;
    }

    if (local_file_mtime < modified_time)
    {
        dfs_log(LL_DEBUG3) << "Local file " << filename << " is older than the server file";
        Fetch(filename);
        return;
    }
}
