//! Admission of generator nodes (openspec: add-generator-cancellation).

use std::sync::Arc;

use remotemedia_traits::{
    is_generative_category, CancelStrategy, GeneratedModality, GeneratorDeclaration,
};
use serde_json::Value;

use crate::nodes::schema::NodeSchema;
use crate::nodes::{StreamingNode, StreamingNodeFactory, StreamingNodeRegistry};
use crate::Error;

/// A factory declaring whatever the test says, creating a plain transform.
struct Declares {
    node_type: &'static str,
    declared: GeneratorDeclaration,
}

struct Transform;
#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for Transform {
    fn node_type(&self) -> &str {
        "Transform"
    }
    async fn process(
        &self,
        data: remotemedia_types::RuntimeData,
    ) -> crate::Result<remotemedia_types::RuntimeData> {
        Ok(data)
    }
}

impl StreamingNodeFactory for Declares {
    fn create(
        &self,
        _node_id: String,
        _params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(
            Transform,
        ))))
    }
    fn node_type(&self) -> &str {
        self.node_type
    }
    fn generator_declaration(&self, _params: &Value) -> GeneratorDeclaration {
        self.declared
    }
}

#[test]
fn a_generator_without_a_cancel_strategy_is_refused() {
    let mut registry = StreamingNodeRegistry::new();
    registry.register(Arc::new(Declares {
        node_type: "NoCancelChat",
        declared: GeneratorDeclaration {
            generates: Some(GeneratedModality::Text),
            cancel: None,
        },
    }));
    assert!(!registry.has_node_type("NoCancelChat"));
    let error = match registry.create_node("NoCancelChat", "n".into(), &Value::Null, None) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("a refused node type was created"),
    };
    assert!(
        error.contains("NoCancelChat") && error.contains("Cancellable"),
        "{error}"
    );
}

#[test]
fn a_node_that_disagrees_with_its_factory_is_not_created() {
    let mut registry = StreamingNodeRegistry::new();
    registry.register(Arc::new(Declares {
        node_type: "ClaimsAudio",
        declared: GeneratorDeclaration {
            generates: Some(GeneratedModality::Audio),
            cancel: Some(CancelStrategy::ObservesToken),
        },
    }));
    // The factory declares a generator; the node it creates declares nothing.
    let error = match registry.create_node("ClaimsAudio", "n".into(), &Value::Null, None) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("a mismatched node was created"),
    };
    assert!(error.contains("ClaimsAudio"), "{error}");
}

#[test]
fn transforms_are_unaffected() {
    let mut registry = StreamingNodeRegistry::new();
    registry.register(Arc::new(Declares {
        node_type: "Resample",
        declared: GeneratorDeclaration::NONE,
    }));
    assert!(registry
        .create_node("Resample", "n".into(), &Value::Null, None)
        .is_ok());
}

/// Built-in node types in a generative category that do not declare a
/// generator role yet. Each is a migration in tasks.md section 3; this list
/// may only shrink. A node that declares its role must leave it.
const NOT_YET_MIGRATED: &[&str] = &[];

/// Node types in a generative category that were reviewed and are
/// transforms, not generators.
const TRANSFORMS: &[&str] = &[
    // Buffers renderer envelopes until the renderer warms up, then paces them.
    "RenderReadyGateNode",
    // Observes avatar intents on the bus.
    "AvatarIntentTapNode",
    // Map one affect input (V/A/D, hidden-state projections) to one
    // blendshape frame, sub-millisecond, nothing queued or running.
    "VadToFaceNode",
    "ActivationFaceNode",
];

/// The audit (tasks 2.8): every built-in node in a generative category
/// declares a generator role, unless it is listed above.
#[test]
fn every_builtin_generator_declares_its_role() {
    let registry = crate::nodes::streaming_registry::create_default_streaming_registry();
    let schemas: Vec<NodeSchema> = registry.collect_schemas();
    let mut undeclared = Vec::new();
    let mut migrated_but_listed = Vec::new();
    for schema in &schemas {
        let generative = schema
            .category
            .as_deref()
            .is_some_and(is_generative_category);
        let declared = registry
            .get_factory(&schema.node_type)
            .map(|f| f.generator_declaration(&Value::Object(Default::default())))
            .unwrap_or_default()
            .is_generator()
            || schema.generator_declaration().is_generator();
        let listed = NOT_YET_MIGRATED.contains(&schema.node_type.as_str());
        let transform = TRANSFORMS.contains(&schema.node_type.as_str());
        if generative && !declared && !listed && !transform {
            undeclared.push(format!(
                "{} (category {:?})",
                schema.node_type, schema.category
            ));
        }
        if declared && listed {
            migrated_but_listed.push(schema.node_type.clone());
        }
    }
    assert!(
        undeclared.is_empty(),
        "generator nodes without a declared role (implement Cancellable and return a GeneratorRole): {undeclared:?}"
    );
    assert!(
        migrated_but_listed.is_empty(),
        "remove these from NOT_YET_MIGRATED, they declare their role now: {migrated_but_listed:?}"
    );
}

/// A generator declared through `#[node(generates = ..., cancel = ...)]`:
/// `generates` is caps DSL, so one attribute gives the role and the output
/// capability. In its own module: the macro's output names `Result` with two
/// parameters.
mod declared_by_the_attribute {
    use remotemedia_traits::{
        AudioGenerator, CancelStrategy, Cancellable, GeneratedModality, ImageGenerator,
    };

    #[crate::node(
        node_type = "MacroTts",
        generates = "audio(sample_rate=24000, channels=1)",
        cancel = "observes_token"
    )]
    pub struct MacroTtsNode {
        #[config(default = 1)]
        pub voice: u32,
    }

    impl MacroTtsNode {
        async fn process_impl(
            &self,
            data: remotemedia_types::RuntimeData,
        ) -> Result<remotemedia_types::RuntimeData, crate::Error> {
            Ok(data)
        }
    }

    #[async_trait::async_trait]
    impl Cancellable for MacroTtsNode {
        fn cancel_strategy(&self) -> CancelStrategy {
            CancelStrategy::ObservesToken
        }
    }
    impl AudioGenerator for MacroTtsNode {}

    #[crate::node(
        node_type = "MacroPainter",
        generates = "image(format=[png, jpeg])",
        cancel = "drop_stops_work"
    )]
    pub struct MacroPainterNode {}

    impl MacroPainterNode {
        async fn process_impl(
            &self,
            data: remotemedia_types::RuntimeData,
        ) -> Result<remotemedia_types::RuntimeData, crate::Error> {
            Ok(data)
        }
    }

    #[async_trait::async_trait]
    impl Cancellable for MacroPainterNode {
        fn cancel_strategy(&self) -> CancelStrategy {
            CancelStrategy::DropStopsWork
        }
    }
    impl ImageGenerator for MacroPainterNode {}

    #[test]
    fn the_attribute_declares_role_and_output() {
        use crate::capabilities::MediaConstraints;
        use crate::nodes::AsyncStreamingNode;
        use remotemedia_traits::schema::NodeConfigSchema;

        let tts = MacroTtsNode::with_default();
        let role = tts.generator().expect("a generator role");
        assert_eq!(role.modality(), GeneratedModality::Audio);
        assert_eq!(
            role.cancellable().cancel_strategy(),
            CancelStrategy::ObservesToken
        );
        assert_eq!(
            MacroTtsNodeConfig::generator(),
            Some((GeneratedModality::Audio, CancelStrategy::ObservesToken))
        );
        let caps = MacroTtsNode::media_capabilities_static();
        let Some(MediaConstraints::Audio(audio)) = caps.default_output() else {
            panic!("{caps:?}");
        };
        assert_eq!(
            audio.sample_rate,
            Some(crate::capabilities::ConstraintValue::Exact(24000))
        );

        let painter = MacroPainterNode::with_default();
        assert_eq!(
            painter.generator().map(|r| r.modality()),
            Some(GeneratedModality::Image)
        );
        assert!(matches!(
            MacroPainterNode::media_capabilities_static().default_output(),
            Some(MediaConstraints::Image(_))
        ));
    }
}
