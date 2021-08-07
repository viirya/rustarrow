cmake_minimum_required(VERSION 3.20)
project(parquet_arrow)

set(CMAKE_CXX_STANDARD 14)

# Don't work on Mac
# find_package(Arrow REQUIRED)
# find_package(Parquet REQUIRED)

# Find library location by looking at the shared libraries, instead
find_path(ARROW_LIB_DIR NAMES libarrow.dylib PATHS /usr/local/lib)
find_path(PARQUET_LIB_DIR NAMES libparquet.dylib PATHS /usr/local/lib)

if(DEFINED ARROW_LIB_DIR AND ARROW_LIB_DIR)
    message("Found arrow lib")
else()
    message("Cannot find arrow lib")
endif()

if(DEFINED PARQUET_LIB_DIR AND PARQUET_LIB_DIR)
    message("Found parquet lib")
else()
    message("Cannot find parquet lib")
endif()

if(DEFINED ARROW_LIB_DIR AND ARROW_LIB_DIR AND DEFINED PARQUET_LIB_DIR AND PARQUET_LIB_DIR)
  find_path(ARROW_INCLUDE_DIR NAMES arrow/api.h)
  find_path(PARQUET_INCLUDE_DIR NAMES parquet/api/io.h)

  add_executable(parquet_arrow main.cpp)

  target_include_directories(parquet_arrow PRIVATE ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR})
  target_link_directories(parquet_arrow PRIVATE ${ARROW_LIB_DIR} ${PARQUET_LIB_DIR})
  target_link_libraries(parquet_arrow libarrow.dylib libparquet.dylib)
endif()
