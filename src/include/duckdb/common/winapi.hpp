//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/winapi.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef DUCKDB_API  // 防止重复定义
#if defined(_WIN32) && !defined(__MINGW32__)  // Windows平台且非MinGW编译器
    #ifdef DUCKDB_STATIC_BUILD  // 静态库构建
        #define DUCKDB_API  // 空定义（静态库不需要导入/导出）
    #else
        #if defined(DUCKDB_BUILD_LIBRARY) && !defined(DUCKDB_BUILD_LOADABLE_EXTENSION)
            #define DUCKDB_API __declspec(dllexport)  // 导出符号（构建核心库时）
        #else
            #define DUCKDB_API __declspec(dllimport)  // 导入符号（使用库时）
        #endif
    #endif
#else  // 非Windows平台（Linux/macOS）或MinGW编译器
    #define DUCKDB_API  // 空定义（默认符号可见）
#endif
#endif

#ifndef DUCKDB_EXTENSION_API
#ifdef _WIN32
#ifdef DUCKDB_STATIC_BUILD
#define DUCKDB_EXTENSION_API
#else
#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_EXTENSION_API __declspec(dllexport)
#else
#define DUCKDB_EXTENSION_API
#endif
#endif
#else
#define DUCKDB_EXTENSION_API __attribute__((visibility("default")))
#endif
#endif
