# WeaselDB
## Motivation

Implement a relational database management system, similar to Postgresql.
Work in progress: indexes, transactions, server/client interface

## Supported SQL

Here is a non-exhuastive list of supported SQL features:

* int4, int8, text, float4, bool
* create / drop table
* insert / update /delete
* select / order by / limit
* aggregate functions avg/count/max/min/sum
* subqueries, correlated and non-correlated
* cross, inner, left, right, full joins
* primary keys, unique constraint, not null constraint

# Building

The Makefile in weasel/src expects to link to a static rocksdb library 
built in the same directory as the weasel project.

/rocksdb
    /include 
    ...
/weasel
    /src
    ...

## Building rocksdb

Follow the instructions for building rocksdb from https://github.com/facebook/rocksdb

## Building and Testing WeaselDB

Run make in weasel/src/ to build weasel
Run test.py in weasel/test 

# Examples

# Architecture

High-level architecture of the current system.

![Weasel Architecture](/weasel_arch.png "Weasel Architecture")

## High-Level
## Tokenizer
## Parser
## Interpreter
## Tables
## Rocksdb storage engine
