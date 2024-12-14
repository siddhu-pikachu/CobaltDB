# CobaltDB - A Simple Database Management System

----
#### Team Members
   - Siddhu Neehal Rapeti
   - Yukta Piyush Shah
   - Sakshi Yawale
   - Gagandeep Singh
   - Himanshi Rohera

#### Query Processing
- Basic query handling skeleton in `App.java`
- Supports basic commands: `create`, `show`, `insert`, `delete`, `select`, `update`, `drop`
- Command-line interface with a simple prompt system

#### Table Management
- `InternalTableRecord.java`: Table operations
  - Models Internal Node Records: Represents a record in a B+ tree's internal node with a row_id (key) and a pointer to the left child page.
  - Supports Tree Structure: Links keys to subtrees, aiding in navigation within the B+ tree.
  - Minimal Implementation: Simple constructor initializes the row_id and LeftChildPgnum attributes without additional methods.

- `TableRecord.java`: Table operations
  - Encapsulates Table Record Data: It represents a single record in a table, storing its row ID, column data types, record    content, and positional metadata like page offsets.
  - Parses Attributes Dynamically: The setAttributes() method splits the record_content into individual fields based on column data types, creating a list of Attribute objects.
  - Manages Byte Conversions: It uses utility methods to convert raw byte arrays to objects and back, enabling efficient serialization and deserialization of the record data.

#### Data Types
- `DataType.java`: Custom datatype implementation
  - Supports standard SQL types (INT, SMALLINT, etc.)
  - Special handling for TEXT type with variable lengths (0-115 characters)
  - Implements cobaltdb type codes (0x00 - 0x0C+)

#### B+ Tree Implementation
- `BPlusTree.java`: Base interface for tree nodes
  - Defines common node operations
  - Inherited by LeafNode and InternalNode

- `Page.java`: Core indexing structure
  - Dynamic page management
  - Handles record insertion
  - Implements page splitting
  - Maintains tree balance

#### Metadata Management
- `AppFileHandler.java`: Basic metadata storage
  - Initial implementation of file handling
  - Needs significant updates to meet cobaltdb specifications
---- 

### Technical Details
- Page Size: 512 bytes
- Data Types: As per cobaltdb specification
- File Format: .tbl for tables, .ndx for indexes (planned)
- B+ Tree: Order 4 implementation

