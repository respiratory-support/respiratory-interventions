import pandas as pd
import numpy as np
import os

from keras.models import Model
from keras.callbacks import EarlyStopping, ModelCheckpoint, LearningRateScheduler
import tensorflow as tf

from data_preprocess_stat_cont_med import load_data

x_train, y_train, x_test, y_test = load_data(7)

def lr_schedule(epoch, lr):
    if epoch % 2 == 0:
        return lr * tf.math.exp(-0.05)
    else:
        return lr
lr_scheduler = LearningRateScheduler(lr_schedule)


rootdir = 'train_mlp_sw'  

HYPERPARAMETERS = {
    "FNN_NODES": [128, 256, 512, 1024],
   'FNN_LAYERS': [3,4,5],
}
random_hyperparameters = list(it.product(*HYPERPARAMETERS.values()))
np.random.shuffle(random_hyperparameters)  
for  FNN_NODES, FNN_LAYERS in random_hyperparameters:
    odir = f'{rootdir}/{FNN_LAYERS}x{FNN_NODES}'
    print('=====================')
    print(odir)
    print('=====================')
    input_stat = Input(batch_shape=(None, x_train[1].shape[-1]))
    input_cont = Input(batch_shape=(None, x_train[0].shape[-1]))
    print(input_cont.shape)
    output = concatenate([input_cont, input_stat])
    for ilayer in range(FNN_LAYERS):
        output = Dense(FNN_NODES)(output)
        output = Dropout(0.2)(output)
    output = Dense(1, activation='sigmoid')(output)
    
    auc = tf.keras.metrics.AUC()
    weight_path = odir + "/tunned_weights.hdf5"
    model = Model(inputs=[input_cont, input_stat], outputs=[output])
    model.compile(loss=tf.keras.losses.BinaryCrossentropy(), optimizer='adam', metrics=[auc])
    
    hist = model.fit(x_train, y_train, validation_split=0.1, epochs=100, batch_size = 512,    
                            callbacks=[EarlyStopping(monitor='val_loss', patience=3, verbose=1, mode='auto'),
                                       ModelCheckpoint(monitor='val_loss', filepath=weight_path, verbose=1, 
                                                       save_best_only=True),lr_scheduler] 
                            )  
    open(odir + "/model.json", "wt").write(model.to_json()) 
    model.load_weights(weight_path)  
    val_loss = hist.history['val_loss']
    newdir = f'{os.path.dirname(odir)}/{min(val_loss):.3f} {os.path.basename(odir)}'
    os.rename(odir, newdir) 
