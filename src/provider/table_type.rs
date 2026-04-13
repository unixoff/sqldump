use crate::provider::ObjectKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    BaseTable,
    View,
}

impl TableType {
    pub fn from_mysql(value: &str) -> Option<Self> {
        match value {
            "BASE TABLE" => Some(Self::BaseTable),
            "VIEW" => Some(Self::View),
            _ => None,
        }
    }

    pub fn object_kind(self) -> ObjectKind {
        match self {
            Self::BaseTable => ObjectKind::BaseTable,
            Self::View => ObjectKind::View,
        }
    }
}
