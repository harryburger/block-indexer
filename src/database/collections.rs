use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum CollectionName {
    Event, // This will store what was previously called RawEvent
    MemberTransaction,
    MemberBalance,
    ConfigIndexer,
    SyncJob,
    Dao,
    Plugin,
    Token,
}

impl fmt::Display for CollectionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            CollectionName::Event => "Event",
            CollectionName::MemberTransaction => "MemberTransaction",
            CollectionName::MemberBalance => "MemberBalance",
            CollectionName::ConfigIndexer => "ConfigIndexer",
            CollectionName::SyncJob => "SyncJob",
            CollectionName::Dao => "Dao",
            CollectionName::Plugin => "Plugin",
            CollectionName::Token => "Token",
        };
        write!(f, "{name}")
    }
}

impl CollectionName {
    pub fn as_str(&self) -> &'static str {
        match self {
            CollectionName::Event => "Event",
            CollectionName::MemberTransaction => "MemberTransaction",
            CollectionName::MemberBalance => "MemberBalance",
            CollectionName::ConfigIndexer => "ConfigIndexer",
            CollectionName::SyncJob => "SyncJob",
            CollectionName::Dao => "Dao",
            CollectionName::Plugin => "Plugin",
            CollectionName::Token => "Token",
        }
    }

    /// Get the full namespace for bulk operations (database.collection)
    pub fn namespace(&self, database_name: &str) -> String {
        format!("{}.{}", database_name, self.as_str())
    }
}
