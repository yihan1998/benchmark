#ifndef _CORE_WORKLOAD_H_
#define _CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "properties.h"
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "utils.h"

class CoreWorkload {
 public:  
  /// 
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string DISTRIBUTION_PROPERTY;
  static const std::string DISTRIBUTION_DEFAULT;
  
  static const std::string COUNT_PROPERTY;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);
  
  virtual uint64_t NextServiceTime(void);

  CoreWorkload() :
      service_time_chooser_(NULL), count_(0) {
  }
  
  virtual ~CoreWorkload() {
    if (service_time_chooser_) delete service_time_chooser_;
  }
  
 protected:
  Generator<uint64_t> *service_time_chooser_;
  size_t count_;
};

#endif // _CORE_WORKLOAD_H_
