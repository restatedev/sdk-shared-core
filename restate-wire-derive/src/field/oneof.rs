// Adapted from prost-derive 0.14.3 (Apache-2.0), Copyright the prost authors.
// Modifications: oneof helpers dispatch through the inherent methods we
// emit on the oneof enum (`encode`, `merge`, `encoded_len`). No host
// registry is threaded — host-resident lengths come from
// `HostBufferHandle::len`.

use anyhow::{bail, Error};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_str, Expr, ExprLit, Ident, Lit, Meta, MetaNameValue, Path};

use crate::field::{set_option, tags_attr};

#[derive(Clone)]
pub struct Field {
    pub ty: Path,
    pub tags: Vec<u32>,
}

impl Field {
    pub fn new(attrs: &[Meta]) -> Result<Option<Field>, Error> {
        let mut ty = None;
        let mut tags = None;
        let mut unknown_attrs = Vec::new();

        for attr in attrs {
            if attr.path().is_ident("oneof") {
                let t = match *attr {
                    Meta::NameValue(MetaNameValue {
                        value:
                            Expr::Lit(ExprLit {
                                lit: Lit::Str(ref lit),
                                ..
                            }),
                        ..
                    }) => parse_str::<Path>(&lit.value())?,
                    Meta::List(ref list) => list.parse_args::<Ident>()?.into(),
                    _ => bail!("invalid oneof attribute: {attr:?}"),
                };
                set_option(&mut ty, t, "duplicate oneof attribute")?;
            } else if let Some(t) = tags_attr(attr)? {
                set_option(&mut tags, t, "duplicate tags attributes")?;
            } else {
                unknown_attrs.push(attr);
            }
        }

        let ty = match ty {
            Some(ty) => ty,
            None => return Ok(None),
        };

        if !unknown_attrs.is_empty() {
            bail!(
                "unknown attribute(s) for message field: #[prost({})]",
                quote!(#(#unknown_attrs),*)
            );
        }

        let tags = match tags {
            Some(tags) => tags,
            None => bail!("oneof field is missing a tags attribute"),
        };

        Ok(Some(Field { ty, tags }))
    }

    /// Returns a statement which encodes the oneof field.
    pub fn encode(&self, ident: TokenStream) -> TokenStream {
        quote! {
            if let ::core::option::Option::Some(ref oneof) = #ident {
                oneof.encode(buf)
            }
        }
    }

    /// Returns an expression which evaluates to the result of decoding the oneof field.
    pub fn merge(&self, ident: TokenStream) -> TokenStream {
        let ty = &self.ty;
        quote! {
            #ty::merge(#ident, tag, wire_type, buf, ctx)
        }
    }

    /// Returns an expression which evaluates to the encoded length of the oneof field.
    pub fn encoded_len(&self, ident: TokenStream) -> TokenStream {
        let ty = &self.ty;
        quote! {
            #ident.as_ref().map_or(0, |v| #ty::encoded_len(v))
        }
    }

    pub fn clear(&self, ident: TokenStream) -> TokenStream {
        quote!(#ident = ::core::option::Option::None)
    }
}
