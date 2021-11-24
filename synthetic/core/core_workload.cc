#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"

#include <string>

using std::string;

const string CoreWorkload::DISTRIBUTION_PROPERTY ="requestdistribution";
const string CoreWorkload::DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::COUNT_PROPERTY = "count";

void CoreWorkload::Init(const utils::Properties &p) {   
    std::string distribution = p.GetProperty(DISTRIBUTION_PROPERTY, DISTRIBUTION_DEFAULT);

    count_ = std::stoi(p.GetProperty(COUNT_PROPERTY));
    
    if (distribution == "constant") {
        service_time_chooser_ = new ConstGenerator(1);
    } else if (distribution == "uniform") {
        service_time_chooser_ = new UniformGenerator(0, count_ - 1);
    } else if (distribution == "zipfian") {
        service_time_chooser_ = new ZipfianGenerator(1, count_);
    } else {
        throw utils::Exception("Unknown request distribution: " + distribution);
    }
}

uint64_t CoreWorkload::NextServiceTime(void) {
    return service_time_chooser_->Next();
}

