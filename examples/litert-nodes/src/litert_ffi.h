/**
 * TFLite/LiteRT C API wrapper header
 * 
 * This header provides a minimal subset of the TFLite C API needed for
 * Whisper inference. The full API is available at:
 * https://github.com/tensorflow/tensorflow/tree/master/tensorflow/lite/c
 * 
 * For LiteRT (the next-gen TFLite), the API is similar but with "LiteRt" prefix.
 * We support both by conditionally defining aliases.
 */

#ifndef LITERT_FFI_H_
#define LITERT_FFI_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// -----------------------------------------------------------------------------
// TFLite C API (standard)
// -----------------------------------------------------------------------------

// Opaque types
typedef struct TfLiteModel TfLiteModel;
typedef struct TfLiteInterpreter TfLiteInterpreter;
typedef struct TfLiteInterpreterOptions TfLiteInterpreterOptions;
typedef struct TfLiteTensor TfLiteTensor;
typedef struct TfLiteRegistration TfLiteRegistration;
typedef struct TfLiteOpResolver TfLiteOpResolver;

// Tensor types
typedef enum {
  kTfLiteNoType = 0,
  kTfLiteFloat32 = 1,
  kTfLiteInt32 = 2,
  kTfLiteUInt8 = 3,
  kTfLiteInt8 = 4,
  kTfLiteInt16 = 5,
  kTfLiteInt64 = 6,
  kTfLiteString = 7,
  kTfLiteBool = 8,
  kTfLiteFloat16 = 9,
  kTfLiteComplex64 = 10,
  kTfLiteComplex128 = 11,
  kTfLiteFloat64 = 12,
  kTfLiteResource = 13,
  kTfLiteVariant = 14,
} TfLiteType;

// Model functions
const char* TfLiteVersion(void);
TfLiteModel* TfLiteModelCreateFromFile(const char* model_path);
TfLiteModel* TfLiteModelCreate(const void* model_data, size_t model_size);
void TfLiteModelDelete(TfLiteModel* model);

// Interpreter options
TfLiteInterpreterOptions* TfLiteInterpreterOptionsCreate(void);
void TfLiteInterpreterOptionsDelete(TfLiteInterpreterOptions* options);
void TfLiteInterpreterOptionsSetNumThreads(TfLiteInterpreterOptions* options, int num_threads);
void TfLiteInterpreterOptionsSetErrorReporter(TfLiteInterpreterOptions* options, void (*reporter)(void*, const char*, va_list), void* user_data);
void TfLiteInterpreterOptionsAddBuiltinOp(TfLiteInterpreterOptions* options, const TfLiteRegistration* registration);
void TfLiteInterpreterOptionsAddCustomOp(TfLiteInterpreterOptions* options, const TfLiteRegistration* registration, const char* name, uint32_t version);

// Interpreter
TfLiteInterpreter* TfLiteInterpreterCreate(TfLiteModel* model, TfLiteInterpreterOptions* options);
void TfLiteInterpreterDelete(TfLiteInterpreter* interpreter);
int TfLiteInterpreterAllocateTensors(TfLiteInterpreter* interpreter);
int TfLiteInterpreterInvoke(TfLiteInterpreter* interpreter);
int TfLiteInterpreterGetInputTensorCount(TfLiteInterpreter* interpreter);
int TfLiteInterpreterGetOutputTensorCount(TfLiteInterpreter* interpreter);
TfLiteTensor* TfLiteInterpreterGetInputTensor(TfLiteInterpreter* interpreter, int input_index);
TfLiteTensor* TfLiteInterpreterGetOutputTensor(TfLiteInterpreter* interpreter, int output_index);
int TfLiteInterpreterGetInputTensorIndex(TfLiteInterpreter* interpreter, const char* name);
int TfLiteInterpreterGetOutputTensorIndex(TfLiteInterpreter* interpreter, const char* name);

// Tensor functions
const char* TfLiteTensorName(const TfLiteTensor* tensor);
TfLiteType TfLiteTensorType(const TfLiteTensor* tensor);
int TfLiteTensorNumDims(const TfLiteTensor* tensor);
const int* TfLiteTensorDims(const TfLiteTensor* tensor);
size_t TfLiteTensorByteSize(const TfLiteTensor* tensor);
void* TfLiteTensorData(const TfLiteTensor* tensor);
void* TfLiteTensorMutableData(TfLiteTensor* tensor);
int TfLiteTensorNumElements(const TfLiteTensor* tensor);
int TfLiteTensorCopyFromBuffer(TfLiteTensor* tensor, const void* input_data, size_t input_size);
int TfLiteTensorCopyToBuffer(const TfLiteTensor* tensor, void* output_data, size_t output_size);

// OpResolver
TfLiteOpResolver* TfLiteOpResolverCreateBuiltin(void);
void TfLiteOpResolverDelete(TfLiteOpResolver* resolver);
void TfLiteOpResolverAddBuiltin(TfLiteOpResolver* resolver, int builtin_code, const TfLiteRegistration* registration, int min_version, int max_version);
void TfLiteOpResolverAddCustom(TfLiteOpResolver* resolver, const char* name, const TfLiteRegistration* registration, int min_version, int max_version);

// -----------------------------------------------------------------------------
// LiteRT C API (next-gen TFLite) - when available
// These are conditionally available depending on the linked library
// -----------------------------------------------------------------------------

// Opaque types for LiteRT
typedef struct LiteRtModel LiteRtModel;
typedef struct LiteRtInterpreter LiteRtInterpreter;
typedef struct LiteRtTensorHandle LiteRtTensorHandle;
typedef struct LiteRtTensorBuffer LiteRtTensorBuffer;
typedef struct LiteRtOpResolver LiteRtOpResolver;

// LiteRT status
typedef int32_t LiteRtStatus;
#define kLiteRtStatusOk 0
#define kLiteRtStatusErrorUnknown 1
#define kLiteRtStatusErrorInvalidArgument 2
#define kLiteRtStatusErrorNotFound 3
#define kLiteRtStatusErrorUnsupported 4

// LiteRT model functions (if available)
LiteRtStatus LiteRtModelCreateFromFile(const char* model_path, LiteRtModel** model);
LiteRtStatus LiteRtModelCreateFromBuffer(const void* model_data, size_t model_size, LiteRtModel** model);
LiteRtStatus LiteRtModelDelete(LiteRtModel* model);
LiteRtStatus LiteRtModelGetInputCount(LiteRtModel* model, int* count);
LiteRtStatus LiteRtModelGetOutputCount(LiteRtModel* model, int* count);
LiteRtStatus LiteRtModelGetInputName(LiteRtModel* model, int index, const char** name);
LiteRtStatus LiteRtModelGetOutputName(LiteRtModel* model, int index, const char** name);

// LiteRT interpreter functions (if available)
LiteRtStatus LiteRtInterpreterCreate(LiteRtModel* model, LiteRtInterpreter** interpreter);
LiteRtStatus LiteRtInterpreterDelete(LiteRtInterpreter* interpreter);
LiteRtStatus LiteRtInterpreterAllocateTensors(LiteRtInterpreter* interpreter);
LiteRtStatus LiteRtInterpreterInvoke(LiteRtInterpreter* interpreter);
LiteRtStatus LiteRtInterpreterGetInputTensor(LiteRtInterpreter* interpreter, int index, LiteRtTensorHandle** tensor);
LiteRtStatus LiteRtInterpreterGetOutputTensor(LiteRtInterpreter* interpreter, int index, LiteRtTensorHandle** tensor);
LiteRtStatus LiteRtInterpreterGetInputTensorByName(LiteRtInterpreter* interpreter, const char* name, LiteRtTensorHandle** tensor);
LiteRtStatus LiteRtInterpreterGetOutputTensorByName(LiteRtInterpreter* interpreter, const char* name, LiteRtTensorHandle** tensor);

// LiteRT tensor functions (if available)
LiteRtStatus LiteRtTensorHandleGetType(LiteRtTensorHandle* tensor, int* type);
LiteRtStatus LiteRtTensorHandleGetNumDims(LiteRtTensorHandle* tensor, int* num_dims);
LiteRtStatus LiteRtTensorHandleGetDims(LiteRtTensorHandle* tensor, int* dims, int max_dims);
LiteRtStatus LiteRtTensorHandleGetByteSize(LiteRtTensorHandle* tensor, size_t* byte_size);
LiteRtStatus LiteRtTensorHandleGetData(LiteRtTensorHandle* tensor, void** data);
LiteRtStatus LiteRtTensorHandleSetData(LiteRtTensorHandle* tensor, void* data, size_t byte_size);

// Tensor buffer (for custom memory management)
LiteRtStatus LiteRtTensorBufferCreateFromHostMemory(void* data, size_t byte_size, LiteRtTensorBuffer** buffer);
LiteRtStatus LiteRtTensorBufferDelete(LiteRtTensorBuffer* buffer);
LiteRtStatus LiteRtTensorHandleSetBuffer(LiteRtTensorHandle* tensor, LiteRtTensorBuffer* buffer);

#ifdef __cplusplus
}
#endif

#endif  // LITERT_FFI_H_
