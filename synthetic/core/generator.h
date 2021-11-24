//
//  generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/6/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef _GENERATOR_H_
#define _GENERATOR_H_

#include <cstdint>
#include <string>
template <typename Value>
class Generator {
 public:
  virtual Value Next() = 0;
  virtual Value Last() = 0;
  virtual ~Generator() { }
};

#endif // YCSB_C_GENERATOR_H_
