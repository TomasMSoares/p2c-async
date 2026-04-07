#pragma once
#include <string>
#include <memory>
#include <mutex>

// FORWARD DECLARATIONS
namespace anyblob {
    namespace network {
        class TaskedSendReceiverGroup;
        class TaskedSendReceiverHandle;
    }
    namespace cloud {
        class Provider;
    }
}

namespace p2cllvm {

struct Config; // Forward declarations

class AnyBlobManager {
public:
    // Delete copy constructor and assignment
    AnyBlobManager(const AnyBlobManager&) = delete;
    AnyBlobManager& operator=(const AnyBlobManager&) = delete;
    
    static void connect(const Config& cfg);
    static AnyBlobManager& getInstance();

    anyblob::network::TaskedSendReceiverGroup* getGroup() const;
    anyblob::cloud::Provider* getProvider() const;
    const std::string& getProviderUri() const;

    std::unique_ptr<anyblob::network::TaskedSendReceiverHandle> createHandle();
    std::string fetchObject(const std::string& object_key);

private:
    AnyBlobManager() = default;
    static AnyBlobManager& getInstanceInternal();
    void connectImpl(const Config& cfg);

    std::unique_ptr<anyblob::network::TaskedSendReceiverGroup> group;
    std::unique_ptr<anyblob::cloud::Provider> provider;
    std::once_flag init_flag;
    std::string provider_uri;
    bool initialized = false;
};

} // namespace p2cllvm