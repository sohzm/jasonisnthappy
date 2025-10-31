
pub const PAGE_SIZE: usize = 4096;

pub const VERSION: u32 = 1;

pub const MAGIC: [u8; 4] = *b"DEVI";

pub const WAL_MAGIC: [u8; 4] = *b"WLOG";

pub const DEFAULT_CACHE_SIZE: usize = 1000;

pub const BTREE_ORDER: usize = 50;

pub const MIN_KEYS: usize = BTREE_ORDER / 2;

pub const WAL_HEADER_SIZE: usize = 32;

pub const WAL_FRAME_SIZE: usize = PAGE_SIZE + 28;

pub const DOC_ID_LEN_SIZE: usize = 2;
pub const DATA_LEN_SIZE: usize = 4;
pub const OVERFLOW_SIZE: usize = 8;
pub const XMIN_SIZE: usize = 8;
pub const XMAX_SIZE: usize = 8;

pub const FIRST_PAGE_META: usize = DOC_ID_LEN_SIZE + DATA_LEN_SIZE + OVERFLOW_SIZE;
pub const VERSIONED_FIRST_PAGE_META: usize = XMIN_SIZE + XMAX_SIZE + DOC_ID_LEN_SIZE + DATA_LEN_SIZE + OVERFLOW_SIZE;
pub const MAX_FIRST_PAGE_DATA: usize = PAGE_SIZE - FIRST_PAGE_META - 256;
pub const MAX_OVERFLOW_DATA: usize = PAGE_SIZE - OVERFLOW_SIZE;
pub const MAX_VERSIONED_FIRST_PAGE_DATA: usize = PAGE_SIZE - VERSIONED_FIRST_PAGE_META - 256;

pub const MAX_OVERFLOW_CHAIN_LENGTH: usize = 250000;

pub type TransactionID = u64;

pub type PageNum = u64;
