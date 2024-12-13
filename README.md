# CobaltDB - A Simple Database Management System

## Current Implementation Status
----
by - Siddhu Neehal Rapeti
   - Yukta Piyush Shah
   - Sakshi Yawale
   - Gagandeep Singh

#### Query Processing
- Basic query handling skeleton in `App.java`
- Supports basic commands: `.file`, `create`, `show`, `insert`, `delete`, `select`, `update`
- Command-line interface with a simple prompt system

#### Data Storage Layer
- `Record.java`: Record management system
  - Currently hardcoded for employee table structure
  - Supports basic CRUD operations
  - Uses ByteBuffer for serialization/deserialization

- `Page.java`: Page-level operations
  - Fixed page size of 512 bytes
  - Handles record storage and retrieval
  - Manages page headers and record offsets
  - Implements page splitting when full

#### Table Management
- `Table.java`: Table operations
  - Currently specialized for employee table
  - Handles CSV file processing
  - Manages record insertion through B+ tree

#### Data Types
- `DataType.java`: Custom datatype implementation
  - Supports standard SQL types (INT, SMALLINT, etc.)
  - Special handling for TEXT type with variable lengths (0-115 characters)
  - Implements DavisBase type codes (0x00 - 0x0C+)

#### B+ Tree Implementation
- `Node.java`: Base interface for tree nodes
  - Defines common node operations
  - Inherited by LeafNode and InternalNode

- `BPlusTree.java`: Core indexing structure
  - Dynamic page management
  - Handles record insertion
  - Implements page splitting
  - Maintains tree balance

#### Metadata Management
- `FileStorage.java`: Basic metadata storage
  - Initial implementation of file handling
  - Needs significant updates to meet DavisBase specifications
---- 
### To-Do List
1. Generalize Record class to handle any table structure
2. Implement proper metadata storage as per DavisBase specifications
3. Add support for index files
4. Implement remaining SQL commands
5. Add proper error handling and validation
6. Implement transaction support
7. Add support for data constraints
8. Improve CSV processing

### Technical Details
- Page Size: 512 bytes
- Data Types: As per DavisBase specification
- File Format: .tbl for tables, .ndx for indexes (planned)
- B+ Tree: Order 4 implementation

## Usage
Currently supports:
```sql
.file filename.csv
create table tablename (columns)
-- Other commands in development
```

## Note
This is a work in progress implementation following the DavisBase specifications. Many features are currently hardcoded and need to be generalized for proper database functionality.
