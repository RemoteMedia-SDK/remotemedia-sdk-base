use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong, jstring};
use jni::JNIEnv;
use log::{error, info, warn};
use remotemedia_scaling_runner_protocol::{
    ParticipantInputMessage, ParticipantOutputMessage, ParticipantType,
    RegisterParticipantRequest, RegisterSessionRequest,
    ParticipantDataFrame,
    ControlSignal,
    SignalType,
    ControlSignalAck,
    AudioPayload,
    VideoPayload,
    ImagePayload,
    TensorPayload,
    NumpyPayload,
    ControlMessagePayload,
    FilePayload,
    runtime_data_payload::Data as RuntimeDataPayloadData,
    runner_control_client::RunnerControlClient,
    runner_control_server::RunnerControl,
};

use pyo3::prelude::*;