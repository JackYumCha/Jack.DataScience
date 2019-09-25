import tensorflow as tf
import numpy as np
import os.path
import sys
import base64
import time
from tensorflow.python.client import device_lib

import typing

class DynamicObject(dict):
    '''
    Example:
    m = Map({'first_name': 'Eduardo'}, last_name='Pool', age=24, sports=['Soccer'])
    '''

    def __init__(self, *args, **kwargs):
        super(DynamicObject, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.iteritems():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.iteritems():
                self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(DynamicObject, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(DynamicObject, self).__delitem__(key)
        del self.__dict__[key]

class DiagnosticTimer:
    startTime = 0.
    def start(this):
        '''record the start time'''
        this.startTime = time.time()
    def elapsedTimeString(this):
        '''get the string representation of the elapsed time in seconds'''
        return 'ElapsedTime = ' + ('%.3f' % (time.time() - this.startTime))
    def PrintCurrentTime(this):
        print(this.elapsedTimeString())

class FileExtensions:
    @staticmethod
    def ReadLines(filename: str):
        return [line.rstrip('\n') for line in open(filename)]
    @staticmethod
    def decodeBase64(encoded: str):
        return base64.b64decode(encoded.encode('utf-8')).decode('utf-8')
    @staticmethod
    def FileExists(filepath: str):
        return os.path.exists(filepath) and os.path.isfile(filepath)


class TensorFlowExtensions:
    #tf_saver = tf.train.Saver()

    @staticmethod
    def GetAvailableGPUs():
        local_device_protos = device_lib.list_local_devices()
        return [x.name for x in local_device_protos if x.device_type == 'GPU']

    @staticmethod
    def PrintAvailableGPUs():
        print('available GPUs:', TensorFlowExtensions.GetAvailableGPUs())

    @staticmethod
    def Save(sess: tf.Session, filename: str):
        tf.train.Saver().save(sess, filename)

    @staticmethod
    def Load(sess: tf.Session, filename: str):
        '''load the session file if it exists'''
        if FileExtensions.FileExists(filename + '.index') and FileExtensions.FileExists(filename + '.meta'):
            try:
                tf.train.Saver().restore(sess, filename)
            except:
                print('Failure in Loading Mode: '+ filename)

    @staticmethod
    def RunSession(sessionFunction: typing.Callable[[tf.Session], typing.Any], logDevice = False):
        '''
        run session in a with block so as to release resources at the end
        please note that tf.global_variables_initializer() was already invoked before calling the provided funcion
        '''
        with tf.Session(config=tf.ConfigProto(log_device_placement=logDevice)) as sess:
            sess.run(tf.global_variables_initializer())
            sessionFunction(sess)

class IndiceShifter:
    shape = np.array([])
    indices = np.array([])
    size = 0
    def __init__(this, shape: []):
        this.shape = shape
        this.size = len(shape)
        this.indices = np.zeros(this.size, dtype=np.int32)
    def next(this):
        this.indices[this.size - 1] += 1
        return this.shift()
    def nextToZero(this, step: int):
        if step >= this.shape[this.size - 1]:
            this.indices[this.size - 1] += this.shape[this.size - 1]
        else:
            this.indices[this.size - 1] += step
        return this.shiftToZero()
    def shift(this):
        for i in range(0, this.size):
            j = this.size - i - 1
            if this.indices[j] >= this.shape[j]:
                this.indices[j] -= this.shape[j]
                if j > 0:
                    this.indices[j - 1] += 1
                else:
                    return False
        return True
    def shiftToZero(this):
        for i in range(0, this.size):
            j = this.size - i - 1
            if this.indices[j] >= this.shape[j]:
                this.indices[j] = 0
                if j > 0:
                    this.indices[j - 1] += 1
                else:
                    return False
        return True
    def getIndexSliceString(this, length: int = 1):
        result = '['
        for i in range(0, this.size):
            if i < this.size - 1:
                result += str(this.indices[i]).rjust(len(str(this.shape[i]))) + ','
            else:
                if length == 1:
                    result += str(this.indices[i]).rjust(len(str(this.shape[i]))) + ']'
                else:
                    result += str(this.indices[i]).rjust(len(str(this.shape[i]))) + ':' + str(length) + ']'
        return result
    def getMatrixSlice(this, matrix: np.array, length: int = 1):
        slice = matrix
        for i in range(0, this.size):
            if i < this.size - 1:
                slice = slice[this.indices[i]]
            else:
                slice = slice[this.indices[i]:this.indices[i]+length]

        return slice

class ArrayFormatter:
    @staticmethod
    def FormatArray(array: np.array, format: str, length: int):
        '''format the number array and display each number with the same length'''
        list = np.array(array).flatten()
        return ', '.join(map(lambda x: (format % x).rjust(length, ' '), list))
    @staticmethod
    def CompareNpArrays(dict:{}, numberPerRow: int, format: str, length: int):
        '''compare multiple 2D matrices. each of the matrix is assumed to have the same shape'''
        maxNameLength = 0
        shape = None
        for name in dict:
            if len(name) > maxNameLength:
                maxNameLength = len(name)
            shape = dict[name].shape
        if len(shape) == 0:
            return None
        shifter = IndiceShifter(shape)
        while True:
            for name in dict:
                array = dict[name]
                print(name.rjust(maxNameLength),
                      shifter.getIndexSliceString(numberPerRow),
                      ArrayFormatter.FormatArray(shifter.getMatrixSlice(array, numberPerRow), format, length)
                      )
            if not shifter.nextToZero(numberPerRow):
                break

class ConsoleArguments:
    @staticmethod
    def GetArgument(key: str):
        for i in range(0, len(sys.argv) - 1):
            if(sys.argv[i] == key):
                return sys.argv[i + 1]
        return None
    @staticmethod
    def GetIntArgument(key: str):
        for i in range(0, len(sys.argv) - 1):
            if(sys.argv[i] == key):
                return int(sys.argv[i + 1])
        return None
    @staticmethod
    def GetFloatArgument(key: str):
        for i in range(0, len(sys.argv) - 1):
            if(sys.argv[i] == key):
                return float(sys.argv[i + 1])
        return None
    @staticmethod
    def GetBoolArgument(key: str):
        for i in range(0, len(sys.argv) - 1):
            if(sys.argv[i] == key):
                return bool(sys.argv[i + 1])
        return None
    @staticmethod
    def HasArgument(key: str):
        for i in range(0, (len(sys.argv))):
            if(sys.argv[i] == key):
                return True
        return False
    @staticmethod
    def ReadKey(message: str = 'press any key to continue...'):
        input(message)
