include(ExternalProject)
include(FetchContent)

project(readosm_fetch_content)

cmake_minimum_required(VERSION 3.24)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_C_STANDARD 11)

set(READOSM_DIR ${CMAKE_CURRENT_SOURCE_DIR}/opt/libreadosm)
set(READOSM_BIN ${CMAKE_CURRENT_BINARY_DIR}/libreadosm)
set(READOSM_STATIC_LIB ${READOSM_BIN}/lib/libreadosm.a)
set(READOSM_INCLUDES ${READOSM_BIN}/include)

file(MAKE_DIRECTORY ${READOSM_INCLUDES})

set(JEMALLOC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/opt/libjemaloc)
set(JEMALLOC_BIN ${CMAKE_CURRENT_BINARY_DIR}/libjemalloc)
set(JEMALLOC_STATIC_LIB ${JEMALLOC_BIN}/lib/libjemalloc.a)
set(JEMALLOC_INCLUDES ${JEMALLOC_BIN}/include)

file(MAKE_DIRECTORY ${JEMALLOC_INCLUDES})

ExternalProject_Add(
    libreadosm
    PREFIX ${READOSM_BIN}
    SOURCE_DIR ${READOSM_DIR}
    URL https://www.gaia-gis.it/gaia-sins/readosm-1.1.0a.tar.gz
    CONFIGURE_COMMAND ${READOSM_DIR}/configure --srcdir=${READOSM_DIR} --prefix=${READOSM_BIN}
    BUILD_COMMAND make
    BUILD_BYPRODUCTS ${READOSM_STATIC_LIB}
    INSTALL_COMMAND make install

    # DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}
    # BUILD_IN_SOURCE 1
)

ExternalProject_Add(
    libjemalloc
    PREFIX ${JEMLLOC_BIN}
    SOURCE_DIR ${JEMALLOC_DIR}
    URL https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
    CONFIGURE_COMMAND ${JEMALLOC_DIR}/configure --srcdir=${JEMALLOC_DIR} --prefix=${JEMALLOC_BIN}
    BUILD_COMMAND make
    BUILD_BYPRODUCTS ${JEMALLOC_STATIC_LIB}
    INSTALL_COMMAND make install

    # DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}
    # BUILD_IN_SOURCE 1
)

add_library(readosm STATIC IMPORTED GLOBAL)
add_dependencies(readosm libreadosm)

set_target_properties(readosm PROPERTIES IMPORTED_LOCATION ${READOSM_STATIC_LIB})
set_target_properties(readosm PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${JEMALLOC_INCLUDES})

add_library(jemalloc STATIC IMPORTED GLOBAL)
add_dependencies(jemalloc libjemalloc)

set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_STATIC_LIB})
set_target_properties(jemalloc PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${READOSM_INCLUDES})

FetchContent_Declare(
    sqlite
    URL https://github.com/azadkuh/sqlite-amalgamation/archive/refs/tags/3.38.2.zip
)

FetchContent_MakeAvailable(sqlite)

add_executable(main main.c allocations.c spellfix.c)

# add_dependencies(main readosm_fetch)
target_link_libraries(main PRIVATE readosm SQLite3 z expat jemalloc)
