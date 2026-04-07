#pragma once
#include <string>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include "nlohmann/json.hpp"

namespace p2cllvm {

using json = nlohmann::json;

struct Config {
    bool is_remote;
    std::string provider_uri;
    std::string access_key;
    std::string secret_key;
    bool use_ssl;
    std::string dataset_path;
    
    size_t blockpool_size_gb;
    double max_network_bandwidth_gb;
    size_t max_prepared_mb;
    double max_retriever_proportion;
    size_t epoch_duration_ms;
    size_t warm_up_time_ms;
    double damping_factor;
    double ema_alpha;
    double reassignment_threshold;

    Config(const std::string& config_path) {
        std::ifstream cfg(config_path);
        if (!cfg.is_open()) {
            throw std::runtime_error("Error: could not open config file: " + config_path);
        }
        
        json config_json;
        try {
            config_json = json::parse(cfg);
        } catch (json::parse_error& e) {
            throw std::runtime_error("Failed to parse JSON config: " + std::string(e.what()));
        }

        is_remote = config_json["is_remote"];
        provider_uri = config_json["provider_uri"];
        access_key = config_json["access_key"];
        secret_key = config_json["secret_key"];
        use_ssl = config_json["use_ssl"];
        dataset_path = config_json["dataset_path"];
        
        blockpool_size_gb = config_json["blockpool_size_gb"];
        if (blockpool_size_gb == 0) throw std::runtime_error("blockpool_size_gb must be > 0.");

        max_network_bandwidth_gb = config_json["max_network_bandwidth_gb"];
        if (max_network_bandwidth_gb == 0) throw std::runtime_error("max_network_bandwidth_gb must be > 0.");

        max_prepared_mb = config_json["max_prepared_mb"];
        if (max_prepared_mb == 0) throw std::runtime_error("max_prepared_mb must be > 0.");

        max_retriever_proportion = config_json["max_retriever_proportion"];
        if (max_retriever_proportion < 0.0 || max_retriever_proportion > 1.0) {
            throw std::runtime_error("initial_retriever_proportion must be between 0 and 1.");
        }

        epoch_duration_ms = config_json["epoch_duration_ms"];
        if (epoch_duration_ms == 0) throw std::runtime_error("epoch_duration_ms must be > 0.");

        warm_up_time_ms = config_json["warm_up_time_ms"];
        if (warm_up_time_ms == 0) throw std::runtime_error("warm_up_time_ms must be > 0.");

        damping_factor = config_json["damping_factor"];
        if (damping_factor < 0.0 || damping_factor > 1.0) {
            throw std::runtime_error("damping_factor must be between 0 and 1.");
        }

        ema_alpha = config_json["ema_alpha"];
        if (ema_alpha < 0.0 || ema_alpha > 1.0) {
            throw std::runtime_error("ema_alpha must be between 0 and 1.");
        }

        reassignment_threshold = config_json["reassignment_threshold"];
        if (reassignment_threshold < 0.0 || reassignment_threshold > 1.0) {
            throw std::runtime_error("reassignment_threshold must be between 0 and 1.");
        }
    }

    // Loads the singleton instance. Thread-safe initialization.
    static void load(const std::string& path) {
        static std::once_flag flag;
        std::call_once(flag, [&](const std::string& p) {
            instance = std::make_unique<Config>(p);
        }, path);
    }

    // Returns the singleton instance. Throws if not loaded.
    static const Config& get() {
        if (!instance) {
            throw std::runtime_error("Config not initialized! Call Config::load() first.");
        }
        return *instance;
    }

private:
    inline static std::unique_ptr<Config> instance = nullptr;
};

} // namespace p2cllvm