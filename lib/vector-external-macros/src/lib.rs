//! Derive owned, checked conversions from external structs to Vector log events.
use std::collections::HashSet;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote, quote_spanned};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, LitStr, Path};

/// Implements `TryFrom<Self>` for Vector's `Event`.
///
/// Supports owned scalar fields and one level of `Vec`, `HashMap<String, T>`, or
/// `BTreeMap<String, T>`. Conversion rules live in `event::external`.
/// Field names may be overridden with `#[external(rename = "name")]`.
/// Serde attributes do not affect this derive.
///
/// By default the event module is resolved from a dependency on `vector-runtime`, `vector-lib` or
/// `vector-core`, including renamed dependencies. A facade can be selected with
/// `#[external(event_module = "my_runtime::event")]`; it must re-export Vector's
/// event types and the `external` support module.
///
/// ```
/// use vector_external_macros::ExternalSourceEvent;
/// use vector_core::event::Event;
///
/// #[derive(ExternalSourceEvent)]
/// struct Allocation { pid: u32, size: u64 }
/// let event = Event::try_from(Allocation { pid: 123, size: 4096 }).unwrap();
/// assert!(matches!(event, Event::Log(_)));
/// ```
///
/// Unsupported fields are rejected by the conversion trait:
///
/// ```compile_fail
/// use vector_external_macros::ExternalSourceEvent;
/// #[derive(ExternalSourceEvent)]
/// struct Unsupported { value: Option<u64> }
/// ```
///
/// Containers cannot contain other containers:
///
/// ```compile_fail
/// use vector_external_macros::ExternalSourceEvent;
/// #[derive(ExternalSourceEvent)]
/// struct Nested { values: Vec<Vec<u64>> }
/// ```
///
/// Map keys must be owned strings:
///
/// ```compile_fail
/// use vector_external_macros::ExternalSourceEvent;
/// #[derive(ExternalSourceEvent)]
/// struct BadKey { values: std::collections::HashMap<u32, String> }
/// ```
#[proc_macro_derive(ExternalSourceEvent, attributes(external))]
pub fn derive_external_source_event(input: TokenStream) -> TokenStream {
    expand(parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "ExternalSourceEvent does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "ExternalSourceEvent requires a struct with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "ExternalSourceEvent requires a struct with named fields",
            ))
        }
    };

    let mut event_module: Option<Path> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("external") {
            attr.parse_nested_meta(|meta| {
                if !meta.path.is_ident("event_module") {
                    return Err(meta.error("expected `event_module = \"path::to::event\"`"));
                }
                if event_module.is_some() {
                    return Err(meta.error("duplicate event_module attribute"));
                }
                event_module = Some(meta.value()?.parse::<LitStr>()?.parse()?);
                Ok(())
            })?;
        }
    }

    // Parse all field attributes before resolving dependencies so syntax errors
    // remain useful even when the caller has not configured Vector yet.
    let mut names = HashSet::new();
    let mut parsed_fields = Vec::new();
    for field in fields {
        let ident = field.ident.as_ref().expect("named field");
        let mut rename: Option<LitStr> = None;
        for attr in &field.attrs {
            if attr.path().is_ident("external") {
                attr.parse_nested_meta(|meta| {
                    if !meta.path.is_ident("rename") {
                        return Err(meta.error("expected `rename = \"field_name\"`"));
                    }
                    if rename.is_some() {
                        return Err(meta.error("duplicate rename attribute"));
                    }
                    rename = Some(meta.value()?.parse()?);
                    Ok(())
                })?;
            }
        }
        let rust_name = ident.to_string();
        let name = rename.unwrap_or_else(|| {
            LitStr::new(
                rust_name.strip_prefix("r#").unwrap_or(&rust_name),
                ident.span(),
            )
        });
        if !names.insert(name.value()) {
            return Err(syn::Error::new_spanned(
                name,
                "duplicate external field name",
            ));
        }
        parsed_fields.push((field, name));
    }

    let event = match event_module {
        Some(path) => quote!(#path),
        None => resolve_event_module()?,
    };
    let name = &input.ident;
    let inserts = parsed_fields
        .iter()
        .enumerate()
        .map(|(index, (field, key))| {
            let ident = field.ident.as_ref().expect("named field");
            let local = format_ident!("__external_field_{index}");
            quote_spanned! {field.ty.span()=>
                let #local = #event::external::IntoExternalValue::into_external_value(
                    __external_input.#ident
                ).map_err(|error| error.at_field(#key))?;
                __external_fields.insert(::core::convert::Into::into(#key), #local);
            }
        });

    Ok(quote! {
        impl ::core::convert::TryFrom<#name> for #event::Event {
            type Error = #event::external::EncodeError;

            fn try_from(__external_input: #name) -> ::core::result::Result<Self, Self::Error> {
                let mut __external_fields = #event::ObjectMap::new();
                #(#inserts)*
                ::core::result::Result::Ok(#event::Event::Log(
                    #event::LogEvent::from(__external_fields)
                ))
            }
        }
    })
}

fn resolve_event_module() -> syn::Result<TokenStream2> {
    for package in ["vector-runtime", "vector-lib", "vector-core"] {
        if let Ok(found) = crate_name(package) {
            return Ok(match found {
                // Integration tests/examples are separate crates in the same package.
                FoundCrate::Itself if package == "vector-runtime" => {
                    quote!(::vector_runtime::event)
                }
                FoundCrate::Itself => quote!(crate::event),
                FoundCrate::Name(name) => {
                    let ident = format_ident!("{name}");
                    quote!(::#ident::event)
                }
            });
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "ExternalSourceEvent requires vector-runtime/vector-lib/vector-core or #[external(event_module = \"path::to::event\")]",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn rejects_invalid_inputs() {
        for (input, message) in [
            (
                parse_quote!(
                    enum Bad {
                        A,
                    }
                ),
                "named fields",
            ),
            (
                parse_quote!(
                    struct Bad(u64);
                ),
                "named fields",
            ),
            (
                parse_quote!(
                    struct Bad;
                ),
                "named fields",
            ),
            (
                parse_quote!(
                    struct Bad<T> {
                        value: T,
                    }
                ),
                "generic",
            ),
            (
                parse_quote!(
                    struct Bad {
                        #[external(skip)]
                        value: u64,
                    }
                ),
                "rename",
            ),
            (
                parse_quote!(
                    struct Bad {
                        a: u64,
                        #[external(rename = "a")]
                        b: u64,
                    }
                ),
                "duplicate external field",
            ),
            (
                parse_quote!(
                    struct Bad {
                        #[external(rename = "a", rename = "b")]
                        a: u64,
                    }
                ),
                "duplicate rename",
            ),
        ] {
            assert!(expand(input).unwrap_err().to_string().contains(message));
        }
    }

    #[test]
    fn generates_valid_impl_with_explicit_module() {
        let output = expand(parse_quote! {
            #[external(event_module = "facade::event")]
            struct Input { r#type: String, #[external(rename = "process.pid")] pid: u32 }
        })
        .unwrap();
        syn::parse2::<syn::ItemImpl>(output).unwrap();
    }
}
