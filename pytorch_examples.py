#!/usr/bin/env python3
import sys
sys.path.insert(0, '/mnt/project')

from api import TaskResult, unpack_args
import time


# ============================================================
# PYTORCH INFERENCE HANDLERS
# ============================================================
def pytorch_load_model_handler(slot, ctx):
    model_name = slot.args[0]
    
    # Check if model already loaded
    if not hasattr(ctx, 'models'):
        ctx.models = {}
    
    if model_name in ctx.models:
        if ctx.log_func:
            ctx.log_func(f"Model {model_name} already loaded")
        return TaskResult(success=True, value={'status': 'cached'})
    
    # Simulate model loading
    if ctx.log_func:
        ctx.log_func(f"Worker {ctx.worker_id} loading model {model_name}...")
    
    time.sleep(0.1)
    
    # In real implementation:
    # import torch
    # model = torch.load(f"{model_name}.pth")
    # model.eval()
    # ctx.models[model_name] = model
    
    ctx.models[model_name] = {'loaded': True, 'name': model_name}
    
    if ctx.log_func:
        ctx.log_func(f"Model {model_name} loaded successfully")
    
    return TaskResult(success=True, value={'status': 'loaded', 'model': model_name})


def pytorch_infer_handler(slot, ctx):
    '''
    In real implementation, input data would come from:
     - Shared memory via app.share()
     - Pool via c_args resolution
     - Direct args for simple inputs
    '''
    model_id = slot.args[0]
    batch_size = slot.args[1] if len(slot.args) > 1 else 1
    
    model_names = {0: 'resnet50', 1: 'vgg16', 2: 'mobilenet'}
    model_name = model_names.get(model_id, 'unknown')
    
    if not hasattr(ctx, 'models'):
        ctx.models = {}
    
    if model_name not in ctx.models:
        if ctx.log_func:
            ctx.log_func(f"Auto-loading {model_name}...")
        time.sleep(0.1)
        ctx.models[model_name] = {'loaded': True}
    
    inference_time = 0.02 * batch_size
    time.sleep(inference_time)
    
    result = {
        'model': model_name,
        'batch_size': batch_size,
        'inference_time': inference_time,
        'predictions': list(range(batch_size))  # Fake predictions
    }
    
    if ctx.log_func:
        ctx.log_func(f"Inferred batch of {batch_size} with {model_name}")
    
    return TaskResult(success=True, value=result)


def pytorch_image_classification_handler(slot, ctx):
    image_id = slot.args[0]
    
    if not hasattr(ctx, 'classifier_model'):
        if ctx.log_func:
            ctx.log_func("Loading classifier model...")
        time.sleep(0.15)
        
        ctx.classifier_model = {'loaded': True}
        if ctx.log_func:
            ctx.log_func("Classifier ready")
    
    time.sleep(0.03)
    
    import random
    random.seed(image_id)
    
    classes = ['cat', 'dog', 'bird', 'fish', 'horse']
    prediction = random.choice(classes)
    confidence = random.uniform(0.7, 0.99)
    
    result = {
        'image_id': image_id,
        'prediction': prediction,
        'confidence': confidence,
        'top_5': [random.choice(classes) for _ in range(5)]
    }
    
    if ctx.log_func:
        ctx.log_func(f"Image {image_id}: {prediction} ({confidence:.2%})")
    
    return TaskResult(success=True, value=result)


def pytorch_text_embedding_handler(slot, ctx):
    strings = unpack_args(slot.c_args)
    
    if not strings:
        return TaskResult(success=False, error="No text provided")
    
    text = strings[0]
    
    if not hasattr(ctx, 'embedding_model'):
        if ctx.log_func:
            ctx.log_func("Loading embedding model...")
        time.sleep(0.2)
        
        ctx.embedding_model = {'loaded': True}
    
    time.sleep(0.04)
    
    import random
    random.seed(hash(text) % 10000)
    embedding = [random.random() for _ in range(768)]
    
    result = {
        'text': text[:50],
        'embedding_dim': len(embedding),
        'embedding': embedding[:10]
    }
    
    if ctx.log_func:
        ctx.log_func(f"Embedded text: '{text[:30]}...'")
    
    return TaskResult(success=True, value=result)


def pytorch_object_detection_handler(slot, ctx):
    '''
    Object detection with PyTorch.
    
    Detects multiple objects in image.
    '''
    image_id = slot.args[0]
    
    if not hasattr(ctx, 'detection_model'):
        if ctx.log_func:
            ctx.log_func("Loading object detection model...")
        time.sleep(0.25)
        
        ctx.detection_model = {'loaded': True}
    
    time.sleep(0.05)
    
    # Fake detections
    import random
    random.seed(image_id)
    
    num_objects = random.randint(1, 5)
    detections = []
    
    for i in range(num_objects):
        detections.append({
            'class': random.choice(['person', 'car', 'dog', 'cat', 'bicycle']),
            'confidence': random.uniform(0.6, 0.99),
            'bbox': [
                random.randint(0, 800),
                random.randint(0, 600),
                random.randint(50, 200),
                random.randint(50, 200)
            ]
        })
    
    result = {
        'image_id': image_id,
        'num_objects': num_objects,
        'detections': detections
    }
    
    if ctx.log_func:
        ctx.log_func(f"Image {image_id}: detected {num_objects} objects")
    
    return TaskResult(success=True, value=result)


def pytorch_sentiment_analysis_handler(slot, ctx):
    '''
    Sentiment analysis with PyTorch.
    Classifies text as positive/negative/neutral.
    '''
    strings = unpack_args(slot.c_args)
    
    if not strings:
        return TaskResult(success=False, error="No text")
    
    text = strings[0]
    
    if not hasattr(ctx, 'sentiment_model'):
        if ctx.log_func:
            ctx.log_func("Loading sentiment model...")
        time.sleep(0.15)
        ctx.sentiment_model = {'loaded': True}
    
    time.sleep(0.02)
    
    # Fake sentiment
    import random
    random.seed(hash(text))
    
    sentiments = ['positive', 'negative', 'neutral']
    sentiment = random.choice(sentiments)
    confidence = random.uniform(0.6, 0.95)
    
    result = {
        'text': text[:100],
        'sentiment': sentiment,
        'confidence': confidence
    }
    
    if ctx.log_func:
        ctx.log_func(f"Sentiment: {sentiment} ({confidence:.2%})")
    
    return TaskResult(success=True, value=result)


def pytorch_style_transfer_handler(slot, ctx):
    '''
    Neural style transfer.
    Apply artistic style to image.
    '''
    image_id = slot.args[0]
    style = slot.args[1]
    
    styles = {0: 'picasso', 1: 'vangogh', 2: 'monet'}
    style_name = styles.get(style, 'unknown')
    
    if not hasattr(ctx, 'style_models'):
        if ctx.log_func:
            ctx.log_func("Loading style transfer models...")
        time.sleep(0.3)
        ctx.style_models = {}
    
    if style_name not in ctx.style_models:
        time.sleep(0.1)
        ctx.style_models[style_name] = {'loaded': True}
    
    time.sleep(0.15)
    
    result = {
        'image_id': image_id,
        'style': style_name,
        'processing_time': 0.15,
        'output_size': (1024, 768)
    }
    
    if ctx.log_func:
        ctx.log_func(f"Applied {style_name} style to image {image_id}")
    
    return TaskResult(success=True, value=result)


def pytorch_batch_inference_handler(slot, ctx):
    batch_size = slot.args[0]
    model_id = slot.args[1]
    
    if not hasattr(ctx, 'batch_models'):
        ctx.batch_models = {}
    
    if model_id not in ctx.batch_models:
        time.sleep(0.12)
        ctx.batch_models[model_id] = {'loaded': True}
    
    # Batch inference is more efficient than individual
    # Time per sample decreases with batch size
    time_per_sample = 0.02 / (1 + batch_size * 0.1)
    total_time = time_per_sample * batch_size
    
    time.sleep(total_time)
    
    result = {
        'batch_size': batch_size,
        'model_id': model_id,
        'total_time': total_time,
        'time_per_sample': time_per_sample,
        'throughput': batch_size / total_time
    }
    
    if ctx.log_func:
        ctx.log_func(f"Batch {batch_size}: {result['throughput']:.1f} samples/sec")
    
    return TaskResult(success=True, value=result)


# ============================================================
# HANDLER REGISTRY
# ============================================================

# Export Handlers
HANDLERS = {
    0xB000: pytorch_load_model_handler,
    0xB001: pytorch_infer_handler,
    0xB002: pytorch_image_classification_handler,
    0xB003: pytorch_text_embedding_handler,
    0xB004: pytorch_object_detection_handler,
    0xB005: pytorch_sentiment_analysis_handler,
    0xB006: pytorch_style_transfer_handler,
    0xB007: pytorch_batch_inference_handler,
}


# ============================================================
# USAGE EXAMPLES
# ============================================================

if __name__ == "__main__":
    from api import MpopApi
    
    print("\n" + "="*60)
    print("PYTORCH INFERENCE EXAMPLES")
    print("="*60)
    
    app = MpopApi(
        workers=4,
        display=False,
        handler_module=__name__,
        debug_delay=0.0, #
    )
    
    print("\nEx 1. Image clarification")
    for i in range(50):
        app.enqueue(fn_id=0xB002, args=(i, 0), tsk_id=i)
    
    print("\nEx 2. Word Processing")
    texts = ["hello world", "machine learning", "parallel processing",
             "deep learning", "neural networks"]
    for i, text in enumerate(texts * 4):
        app.enqueue(fn_id=0xB003, c_args=text.encode() + b'\x00', tsk_id=i + 100)
    
    print("\n Ex 3. Object detection (30 images)")
    for i in range(30):
        app.enqueue(fn_id=0xB004, args=(i, 0), tsk_id=i + 200)
    
    print("\n4. Emotion analysis (40 texts)")
    reviews = [
        b"This product is amazing!\x00",
        b"Terrible experience, would not recommend.\x00",
        b"It's okay, nothing special.\x00",
        b"Best purchase ever!\x00",
        b"Disappointed with quality.\x00",
    ]
    for i in range(40):
        app.enqueue(fn_id=0xB005, c_args=reviews[i % len(reviews)], tsk_id=i + 300)
    
   
    print("\n Ex 1. Image clarification")
    for batch_size in [1, 4, 8, 16, 32]:
        for i in range(3):
            app.enqueue(fn_id=0xB007, args=(batch_size, i), tsk_id=i + 400 + batch_size * 10)
    
    app.run()
    
    print("\n All PyTorch examples completed!")
