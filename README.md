# Database

## Purpose
The aim of this project is to build a Redis-like small database server for storing, updating, and retrieving data to enhance user experience and allow easy storage of data without any loss.

## Library
We will be using Gevent for this project which is a Python library that provides a high-level framework for concurrent programming.

## Commands
The server will be able to respond to the following commands:

-   GET  `<key>`
-   SET  `<key>`  `<value>`
-   DELETE  `<key>`
-   FLUSH
-   MGET  `<key1>`  ...  `<keyn>`
-   MSET  `<key1>`  `<value1>`  ...  `<keyn>`  `<valuen>`

## Supported Data Types

-  Strings  _and_  Binary Data
-   Numbers
-  Error messages
-   NULL
-   Arrays 
-   Dictionaries    