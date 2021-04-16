from glob import glob
import tensorflow as tf
from tqdm import tqdm

index = glob("out.tfrecord")

dataset = tf.data.Dataset.from_tensor_slices(index)
dataset = dataset.interleave(tf.data.TFRecordDataset, cycle_length=128, num_parallel_calls=tf.data.experimental.AUTOTUNE, deterministic=False)


def crop_center_and_resize(img, size):
    s = tf.shape(img)
    w, h = s[0], s[1]
    c = tf.maximum(w, h)
    wn, hn = h / c, w / c
    result = tf.image.crop_and_resize(tf.expand_dims(img, 0),
                                      [[(1 - wn) / 2, (1 - hn) / 2, wn, hn]],
                                      [0], [size, size])
    return tf.squeeze(result, 0)


def tf_parse(example_proto):
    features = {
        'image': tf.io.FixedLenFeature([], tf.string)
    }

    str_fields = ["license",
                  "tags",
                  "title",
                  "description",
                  "owner",
                  "img_src"]

    int_fields = ["comment_count",
                  "fave_count",
                  "view_count",
                  "height",
                  "width"]

    for str_field in str_fields:
        features[str_field] = tf.io.FixedLenFeature([], tf.string)

    for int_field in int_fields:
        features[int_field] = tf.io.FixedLenFeature([], tf.int64)

    parsed_features = tf.io.parse_example(example_proto, features)
    parsed_features["image"] = crop_center_and_resize(tf.io.decode_image(parsed_features["image"], channels=3, expand_animations=False), 256)

    return parsed_features


dataset = dataset.map(tf_parse, num_parallel_calls=8, deterministic=False)
dataset = dataset.apply(tf.data.experimental.ignore_errors())
dataset = dataset.apply(tf.data.experimental.dense_to_ragged_batch(16))

for i in tqdm(dataset):
    pass
