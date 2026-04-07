#include "internal/CloudManager.h"
#include "internal/Config.h"

// AnyBlob includes
#include <cloud/provider.hpp>
#include <network/tasked_send_receiver.hpp>
#include <network/transaction.hpp>

#include <stdexcept>

namespace p2cllvm {

using namespace anyblob;

void AnyBlobManager::connect(const Config& cfg) {
    auto& instance = getInstanceInternal();
    std::call_once(instance.init_flag, &AnyBlobManager::connectImpl, &instance, std::cref(cfg));
}

AnyBlobManager& AnyBlobManager::getInstance() {
    auto& instance = getInstanceInternal();
    if (!instance.initialized) {
        throw std::runtime_error("CloudManager accessed before initialization. "
                                 "Driver::initialize() must call AnyBlobManager::connect() first.");
    }
    return instance;
}

AnyBlobManager& AnyBlobManager::getInstanceInternal() {
    static AnyBlobManager instance;
    return instance;
}

void AnyBlobManager::connectImpl(const Config& cfg) {
    if (initialized) return;

    provider_uri = cfg.provider_uri;
    group = std::make_unique<network::TaskedSendReceiverGroup>();
    auto sendReceiverHandle = group->getHandle();

    provider = cloud::Provider::makeProvider(provider_uri, cfg.use_ssl, 
                                             cfg.access_key, cfg.secret_key, 
                                             &sendReceiverHandle);
    
    auto config_provider = provider->getConfig(sendReceiverHandle);
    group->setConfig(config_provider);
    
    initialized = true;
}

network::TaskedSendReceiverGroup* AnyBlobManager::getGroup() const { return group.get(); }

cloud::Provider* AnyBlobManager::getProvider() const { return provider.get(); }

const std::string& AnyBlobManager::getProviderUri() const { return provider_uri; }

std::unique_ptr<network::TaskedSendReceiverHandle> AnyBlobManager::createHandle() {
    return std::make_unique<network::TaskedSendReceiverHandle>(group->getHandle());
}

std::string AnyBlobManager::fetchObject(const std::string& object_key) {
    std::string result;
    bool completed = false;
    std::string error_msg;

    auto callback = [&](anyblob::network::MessageResult& res) {
        if (res.success()) {
            result = std::string(static_cast<const char*>(res.getResult().data()), res.getResult().size());
            completed = true;
        } else {
            error_msg = "Failed to fetch object: " + object_key + 
                        " (Error code: " + std::to_string(res.getFailureCode()) + ")";
        }
    };

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

} // namespace p2cllvm