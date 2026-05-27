// Adapted from prost-derive 0.14.3 (Apache-2.0), Copyright the prost authors.
//
// `#[derive(restate_wire_derive::Message)]` emits the same shape of impl as
// `#[derive(prost::Message)]`, but for our custom
// `::restate_sdk_shared_core::proto::Message` trait. Key differences:
//
//   - `encode_raw<B: RestateBufMut>(&self, buf: &mut B)`   (was `impl BufMut`)
//   - `merge_field<B: RestateBuf>(...)`                    (was `impl Buf`)
//   - `encoded_len(&self)` — host-resident payload lengths come from
//     `HostBufferHandle::len`, so no registry parameter is threaded
//     through encode-side helpers.
//   - New scalar form `bytes = "buffer"` routes through
//     `proto::encode_buffer` / `merge_buffer` / `encoded_len_buffer`.
//
// All emitted paths are hard-coded under
// `::restate_sdk_shared_core::proto::*`. That module re-exports every
// helper the macro needs (including the per-scalar-type prost encoding
// modules like `proto::string`, `proto::uint32`, etc.) so the macro
// output never has to write `::prost::*` paths anywhere.

#![doc(html_root_url = "https://docs.rs/restate-wire-derive/0.10.0")]
#![recursion_limit = "4096"]
#![allow(dead_code)]

use anyhow::{bail, Error};
use itertools::Itertools;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    punctuated::Punctuated, Data, DataEnum, DataStruct, DeriveInput, Fields, FieldsNamed,
    FieldsUnnamed, Ident, Index, Variant,
};

mod field;
use crate::field::Field;

/// The single hard-coded prefix every emitted path uses. The `proto`
/// module of `restate-sdk-shared-core` is the *only* surface the macro
/// references. See `src/proto/mod.rs` (and `src/buffer.rs` for the
/// host-aware sub-traits it re-exports) for the inventory.
fn proto_path() -> TokenStream {
    quote!(::restate_sdk_shared_core::proto)
}

fn try_message(input: TokenStream) -> Result<TokenStream, Error> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = input.ident;

    let variant_data = match input.data {
        Data::Struct(variant_data) => variant_data,
        Data::Enum(..) => bail!("Message can not be derived for an enum"),
        Data::Union(..) => bail!("Message can not be derived for a union"),
    };

    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (is_struct, fields) = match variant_data {
        DataStruct {
            fields: Fields::Named(FieldsNamed { named: fields, .. }),
            ..
        } => (true, fields.into_iter().collect()),
        DataStruct {
            fields:
                Fields::Unnamed(FieldsUnnamed {
                    unnamed: fields, ..
                }),
            ..
        } => (false, fields.into_iter().collect()),
        DataStruct {
            fields: Fields::Unit,
            ..
        } => (false, Vec::new()),
    };

    let mut next_tag: u32 = 1;
    let mut fields = fields
        .into_iter()
        .enumerate()
        .flat_map(|(i, field)| {
            let field_ident = field.ident.map(|x| quote!(#x)).unwrap_or_else(|| {
                let index = Index {
                    index: i as u32,
                    span: Span::call_site(),
                };
                quote!(#index)
            });
            match Field::new(field.attrs, Some(next_tag)) {
                Ok(Some(field)) => {
                    next_tag = field.tags().iter().max().map(|t| t + 1).unwrap_or(next_tag);
                    Some(Ok((field_ident, field)))
                }
                Ok(None) => None,
                Err(err) => Some(Err(
                    err.context(format!("invalid message field {ident}.{field_ident}"))
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Sort the fields by tag number so that fields will be encoded in tag order.
    fields.sort_by_key(|(_, field)| field.tags().into_iter().min().unwrap());
    let fields = fields;

    if let Some(duplicate_tag) = fields
        .iter()
        .flat_map(|(_, field)| field.tags())
        .duplicates()
        .next()
    {
        bail!("message {ident} has multiple fields with tag {duplicate_tag}",)
    };

    let proto = proto_path();

    let encoded_len = fields
        .iter()
        .map(|(field_ident, field)| field.encoded_len(quote!(self.#field_ident)));

    let encode = fields
        .iter()
        .map(|(field_ident, field)| field.encode(quote!(self.#field_ident)));

    let merge = fields.iter().map(|(field_ident, field)| {
        let merge = field.merge(quote!(value));
        let tags = field.tags().into_iter().map(|tag| quote!(#tag));
        let tags = Itertools::intersperse(tags, quote!(|));

        quote! {
            #(#tags)* => {
                let mut value = &mut self.#field_ident;
                #merge.map_err(|mut error| {
                    error.push(STRUCT_NAME, stringify!(#field_ident));
                    error
                })
            },
        }
    });

    let struct_name = if fields.is_empty() {
        quote!()
    } else {
        quote!(
            const STRUCT_NAME: &'static str = stringify!(#ident);
        )
    };

    let clear = fields
        .iter()
        .map(|(field_ident, field)| field.clear(quote!(self.#field_ident)));

    let default = if is_struct {
        let default = fields.iter().map(|(field_ident, field)| {
            let value = field.default();
            quote!(#field_ident: #value,)
        });
        quote! {#ident {
            #(#default)*
        }}
    } else {
        let default = fields.iter().map(|(_, field)| {
            let value = field.default();
            quote!(#value,)
        });
        quote! {#ident (
            #(#default)*
        )}
    };

    let debug_fields = fields
        .iter()
        .map(|(field_ident, _)| quote!(s.field(stringify!(#field_ident), &self.#field_ident);));

    let expanded = quote! {
        impl #impl_generics #proto::Message for #ident #ty_generics #where_clause {
            #[allow(unused_variables)]
            fn encode_raw<B: #proto::RestateBufMut>(&self, buf: &mut B) {
                #(#encode)*
            }

            #[allow(unused_variables)]
            fn merge_field<B: #proto::RestateBuf>(
                &mut self,
                tag: u32,
                wire_type: #proto::WireType,
                buf: &mut B,
                ctx: #proto::DecodeContext,
            ) -> ::core::result::Result<(), #proto::DecodeError>
            {
                #struct_name
                match tag {
                    #(#merge)*
                    _ => #proto::skip_field(wire_type, tag, buf, ctx),
                }
            }

            #[allow(unused_variables)]
            #[inline]
            fn encoded_len(&self) -> usize {
                0 #(+ #encoded_len)*
            }

            fn clear(&mut self) {
                #(#clear;)*
            }
        }

        impl #impl_generics ::core::default::Default for #ident #ty_generics #where_clause {
            fn default() -> Self {
                #default
            }
        }

        #[allow(unused_mut)]
        impl #impl_generics ::core::fmt::Debug for #ident #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                let mut s = f.debug_struct(stringify!(#ident));
                #(#debug_fields)*
                s.finish()
            }
        }
    };

    Ok(expanded)
}

#[proc_macro_derive(Message, attributes(prost))]
pub fn message(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    try_message(input.into()).unwrap().into()
}

fn try_enumeration(input: TokenStream) -> Result<TokenStream, Error> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = input.ident;

    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let punctuated_variants = match input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        Data::Struct(_) => bail!("Enumeration can not be derived for a struct"),
        Data::Union(..) => bail!("Enumeration can not be derived for a union"),
    };

    // Map the variants into 'fields'.
    let mut variants: Vec<(Ident, syn::Expr, Option<TokenStream>)> = Vec::new();
    for Variant {
        attrs,
        ident,
        fields,
        discriminant,
        ..
    } in punctuated_variants
    {
        match fields {
            Fields::Unit => (),
            Fields::Named(_) | Fields::Unnamed(_) => {
                bail!("Enumeration variants may not have fields")
            }
        }
        match discriminant {
            Some((_, expr)) => {
                let deprecated_attr = if attrs.iter().any(|v| v.path().is_ident("deprecated")) {
                    Some(quote!(#[allow(deprecated)]))
                } else {
                    None
                };
                variants.push((ident, expr, deprecated_attr))
            }
            None => bail!("Enumeration variants must have a discriminant"),
        }
    }

    if variants.is_empty() {
        panic!("Enumeration must have at least one variant");
    }

    let (default, _, default_deprecated) = variants[0].clone();

    let is_valid = variants.iter().map(|(_, value, _)| quote!(#value => true));
    let from = variants
        .iter()
        .map(|(variant, value, deprecated)| quote!(#value => ::core::option::Option::Some(#deprecated #ident::#variant)));

    let try_from = variants
        .iter()
        .map(|(variant, value, deprecated)| quote!(#value => ::core::result::Result::Ok(#deprecated #ident::#variant)));

    let is_valid_doc = format!("Returns `true` if `value` is a variant of `{ident}`.");
    let from_i32_doc =
        format!("Converts an `i32` to a `{ident}`, or `None` if `value` is not a valid variant.");

    let proto = proto_path();

    let expanded = quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #[doc=#is_valid_doc]
            pub fn is_valid(value: i32) -> bool {
                match value {
                    #(#is_valid,)*
                    _ => false,
                }
            }

            #[deprecated = "Use the TryFrom<i32> implementation instead"]
            #[doc=#from_i32_doc]
            pub fn from_i32(value: i32) -> ::core::option::Option<#ident> {
                match value {
                    #(#from,)*
                    _ => ::core::option::Option::None,
                }
            }
        }

        impl #impl_generics ::core::default::Default for #ident #ty_generics #where_clause {
            fn default() -> #ident {
                #default_deprecated #ident::#default
            }
        }

        impl #impl_generics ::core::convert::From::<#ident> for i32 #ty_generics #where_clause {
            fn from(value: #ident) -> i32 {
                value as i32
            }
        }

        impl #impl_generics ::core::convert::TryFrom::<i32> for #ident #ty_generics #where_clause {
            type Error = #proto::UnknownEnumValue;

            fn try_from(value: i32) -> ::core::result::Result<#ident, #proto::UnknownEnumValue> {
                match value {
                    #(#try_from,)*
                    _ => ::core::result::Result::Err(#proto::UnknownEnumValue(value)),
                }
            }
        }
    };

    Ok(expanded)
}

#[proc_macro_derive(Enumeration, attributes(prost))]
pub fn enumeration(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    try_enumeration(input.into()).unwrap().into()
}

fn try_oneof(input: TokenStream) -> Result<TokenStream, Error> {
    let input: DeriveInput = syn::parse2(input)?;

    let ident = input.ident;

    let variants = match input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        Data::Struct(..) => bail!("Oneof can not be derived for a struct"),
        Data::Union(..) => bail!("Oneof can not be derived for a union"),
    };

    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Map the variants into 'fields'.
    let mut fields: Vec<(Ident, Field, Option<TokenStream>)> = Vec::new();
    for Variant {
        attrs,
        ident: variant_ident,
        fields: variant_fields,
        ..
    } in variants
    {
        let variant_fields = match variant_fields {
            Fields::Unit => Punctuated::new(),
            Fields::Named(FieldsNamed { named: fields, .. })
            | Fields::Unnamed(FieldsUnnamed {
                unnamed: fields, ..
            }) => fields,
        };
        if variant_fields.len() != 1 {
            bail!("Oneof enum variants must have a single field");
        }
        let deprecated_attr = if attrs.iter().any(|v| v.path().is_ident("deprecated")) {
            Some(quote!(#[allow(deprecated)]))
        } else {
            None
        };
        match Field::new_oneof(attrs)? {
            Some(field) => fields.push((variant_ident, field, deprecated_attr)),
            None => bail!("invalid oneof variant: oneof variants may not be ignored"),
        }
    }

    // Oneof variants cannot be oneofs themselves, so it's impossible to have
    // a field with multiple tags.
    assert!(fields.iter().all(|(_, field, _)| field.tags().len() == 1));

    if let Some(duplicate_tag) = fields
        .iter()
        .flat_map(|(_, field, _)| field.tags())
        .duplicates()
        .next()
    {
        bail!("invalid oneof {ident}: multiple variants have tag {duplicate_tag}");
    }

    let encode = fields.iter().map(|(variant_ident, field, deprecated)| {
        let encode = field.encode(quote!(*value));
        quote!(#deprecated #ident::#variant_ident(ref value) => { #encode })
    });

    let merge = fields.iter().map(|(variant_ident, field, deprecated)| {
        let tag = field.tags()[0];
        let merge = field.merge(quote!(value));
        quote! {
            #deprecated
            #tag => if let ::core::option::Option::Some(#ident::#variant_ident(value)) = field {
                #merge
            } else {
                let mut owned_value = ::core::default::Default::default();
                let value = &mut owned_value;
                #merge.map(|_| *field = ::core::option::Option::Some(#deprecated #ident::#variant_ident(owned_value)))
            }
        }
    });

    let encoded_len = fields.iter().map(|(variant_ident, field, deprecated)| {
        let encoded_len = field.encoded_len(quote!(*value));
        quote!(#deprecated #ident::#variant_ident(ref value) => #encoded_len)
    });

    let debug = fields.iter().map(|(variant_ident, _, deprecated)| {
        quote! {
            #deprecated #ident::#variant_ident(ref value) => {
                f.debug_tuple(concat!(stringify!(#ident), "::", stringify!(#variant_ident)))
                    .field(value)
                    .finish()
            }
        }
    });

    let proto = proto_path();

    let expanded = quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            /// Encodes the message to a buffer.
            pub fn encode<B: #proto::RestateBufMut>(&self, buf: &mut B) {
                match *self {
                    #(#encode,)*
                }
            }

            /// Decodes an instance of the message from a buffer, and merges it into self.
            pub fn merge<B: #proto::RestateBuf>(
                field: &mut ::core::option::Option<#ident #ty_generics>,
                tag: u32,
                wire_type: #proto::WireType,
                buf: &mut B,
                ctx: #proto::DecodeContext,
            ) -> ::core::result::Result<(), #proto::DecodeError>
            {
                match tag {
                    #(#merge,)*
                    _ => unreachable!(concat!("invalid ", stringify!(#ident), " tag: {}"), tag),
                }
            }

            /// Returns the encoded length of the message without a length delimiter.
            #[inline]
            pub fn encoded_len(&self) -> usize {
                match *self {
                    #(#encoded_len,)*
                }
            }
        }

        impl #impl_generics ::core::fmt::Debug for #ident #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                match self {
                    #(#debug,)*
                }
            }
        }
    };

    Ok(expanded)
}

#[proc_macro_derive(Oneof, attributes(prost))]
pub fn oneof(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    try_oneof(input.into()).unwrap().into()
}
