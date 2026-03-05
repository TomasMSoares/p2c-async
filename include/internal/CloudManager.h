#pragma once
#include <fstream>
#include <stdexcept>
#include <string>
#include <memory>
#include <mutex>
#include <iostream>
#include "internal/Config.h"

// AnyBlob includes
#include <cloud/provider.hpp>
#include <network/tasked_send_receiver.hpp>
#include <network/transaction.hpp>

#include "nlohmann/json.hpp"

using namespace anyblob;

namespace p2cllvm {

class AnyBlobManager {
private:
    std::unique_ptr<network::TaskedSendReceiverGroup> group;
    std::unique_ptr<cloud::Provider> provider;
    std::once_flag init_flag;
    std::string provider_uri;
    bool initialized = false;

    AnyBlobManager() = default;

    void connectImpl(const Config& cfg) {
        if (initialized) return;

        const std::string username = cfg.access_key;
        const std::string password = cfg.secret_key;
        bool https = cfg.use_ssl;
        provider_uri = cfg.provider_uri;

        group = std::make_unique<network::TaskedSendReceiverGroup>();
        auto sendReceiverHandle = group->getHandle();

        provider = cloud::Provider::makeProvider(provider_uri, https, username, password, &sendReceiverHandle);
        
        auto config_provider = provider->getConfig(sendReceiverHandle);
        group->setConfig(config_provider);
        
        initialized = true;
    }

public:
    AnyBlobManager(const AnyBlobManager&) = delete;
    AnyBlobManager& operator=(const AnyBlobManager&) = delete;
    
    // Explicit setup function called during bootstrap
    static void connect(const Config& cfg) {
        auto& instance = getInstanceInternal();
        std::call_once(instance.init_flag, &AnyBlobManager::connectImpl, &instance, std::cref(cfg));
    }
    
    static AnyBlobManager& getInstance() {
        auto& instance = getInstanceInternal();
        if (!instance.initialized) {
            throw std::runtime_error("CloudManager accessed before initialization. Driver::initialize() must call AnyBlobManager::connect() first.");
        }
        return instance;
    }

    network::TaskedSendReceiverGroup* getGroup() const { return group.get(); }
    cloud::Provider* getProvider() const { return provider.get(); }
    
    std::unique_ptr<network::TaskedSendReceiverHandle> createHandle() {
        return std::make_unique<network::TaskedSendReceiverHandle>(group->getHandle());
    }

    const std::string& getProviderUri() const { return provider_uri; }

    std::string fetchObject(const std::string& object_key) {
        std::string result;
        bool completed = false;
        std::string error_msg;

        auto callback = [&](anyblob::network::MessageResult& res) {
            if (res.success()) {
                result = std::string(static_cast<const char*>(res.getResult().data()), res.getResult().size());
                completed = true;
            } else {
                error_msg = "Failed to fetch object: " + object_key + " (Error code: " + std::to_string(res.getFailureCode()) + ")";
            }
        };

        // Create a temporary handle for this sync operation
        auto handle = group->getHandle();
        anyblob::network::Transaction txn(provider.get());
        txn.getObjectRequest(callback, object_key);
        
        txn.processSync(handle);

        if (!completed && error_msg.empty()) {
            for (const auto &it : txn) {
                if (it.success()) {
                    result = std::string(static_cast<const char*>(it.getResult().data()), it.getResult().size());
                    completed = true;
                } else {
                    error_msg = "Transaction failed for key: " + object_key;
                }
            }
        }

        if (!completed) {
            throw std::runtime_error(error_msg.empty() ? "Fetch incomplete for key: " + object_key : error_msg);
        }
        
        return result;
    }

private:
    static AnyBlobManager& getInstanceInternal() {
        static AnyBlobManager instance;
        return instance;
    }
};

} // namespace p2cllvm