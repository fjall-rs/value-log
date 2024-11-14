// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(any(not(feature = "bytes"), test))]
mod slice_arc;

#[cfg(any(feature = "bytes", test))]
mod slice_bytes;

#[cfg(not(feature = "bytes"))]
pub use slice_arc::Slice;
#[cfg(feature = "bytes")]
pub use slice_bytes::Slice;

#[cfg(feature = "serde")]
mod serde {
    use super::Slice;
    use serde::de::{self, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;
    use std::ops::Deref;

    impl Serialize for Slice {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(self.deref())
        }
    }

    impl<'de> Deserialize<'de> for Slice {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct SliceVisitor;

            impl<'de> Visitor<'de> for SliceVisitor {
                type Value = Slice;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a byte array")
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Slice, E>
                where
                    E: de::Error,
                {
                    Ok(Slice::from(v))
                }
            }

            deserializer.deserialize_bytes(SliceVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, sync::Arc};

    use super::{slice_arc, slice_bytes};

    fn assert_slice_handles<T>(v: T)
    where
        T: Clone + Debug,
        slice_arc::Slice: From<T> + PartialEq<T> + PartialOrd<T>,
        slice_bytes::Slice: From<T> + PartialEq<T> + PartialOrd<T>,
    {
        // verify slice arc roundtrips
        let slice: slice_arc::Slice = v.clone().into();
        assert_eq!(slice, v, "slice_arc: {slice:?}, v: {v:?}");
        assert!(slice >= v, "slice_arc: {slice:?}, v: {v:?}");

        // verify slice bytes roundtrips
        let slice: slice_bytes::Slice = v.clone().into();
        assert_eq!(slice, v, "slice_bytes: {slice:?}, v: {v:?}");
        assert!(slice >= v, "slice_bytes: {slice:?}, v: {v:?}");
    }

    /// This test verifies that we can create a `Slice` from various types and compare a `Slice` with them.
    #[test]
    fn test_slice_instantiation() {
        // - &[u8]
        assert_slice_handles::<&[u8]>(&[1, 2, 3, 4]);
        // - Arc<u8>
        assert_slice_handles::<Arc<[u8]>>(Arc::new([1, 2, 3, 4]));
        // - Vec<u8>
        assert_slice_handles::<Vec<u8>>(vec![1, 2, 3, 4]);
        // - &str
        assert_slice_handles::<&str>("hello");
        // - String
        assert_slice_handles::<String>("hello".to_string());
        // - [u8; N]
        assert_slice_handles::<[u8; 4]>([1, 2, 3, 4]);

        // Special case for these types
        // - Iterator<Item = u8>
        let slice = slice_arc::Slice::from_iter(vec![1, 2, 3, 4]);
        assert_eq!(slice, vec![1, 2, 3, 4]);
        let slice = slice_bytes::Slice::from_iter(vec![1, 2, 3, 4]);
        assert_eq!(slice, vec![1, 2, 3, 4]);

        // - Arc<str>
        let arc_str: Arc<str> = Arc::from("hello");
        let slice = slice_arc::Slice::from(arc_str.clone());
        assert_eq!(slice.as_ref(), arc_str.as_bytes());
        let slice = slice_bytes::Slice::from(arc_str.clone());
        assert_eq!(slice.as_ref(), arc_str.as_bytes());

        // - io::Read
        let reader = std::io::Cursor::new(vec![1, 2, 3, 4]);
        let slice = slice_arc::Slice::from_reader(&mut reader.clone(), 4).expect("read");
        assert_eq!(slice, vec![1, 2, 3, 4]);
        let slice = slice_bytes::Slice::from_reader(&mut reader.clone(), 4).expect("read");
        assert_eq!(slice, vec![1, 2, 3, 4]);
    }
}
