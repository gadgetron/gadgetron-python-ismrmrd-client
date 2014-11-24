import ismrmrd
import ismrmrd_serial

import struct
import socket
import threading
import numpy as np

GADGET_MESSAGE_INT_ID_MIN                             =   0
GADGET_MESSAGE_CONFIG_FILE                            =   1
GADGET_MESSAGE_CONFIG_SCRIPT                          =   2
GADGET_MESSAGE_PARAMETER_SCRIPT                       =   3
GADGET_MESSAGE_CLOSE                                  =   4
GADGET_MESSAGE_INT_ID_MAX                             = 999
GADGET_MESSAGE_EXT_ID_MIN                             = 1000
GADGET_MESSAGE_ACQUISITION                            = 1001 # DEPRECATED
GADGET_MESSAGE_NEW_MEASUREMENT                        = 1002 # DEPRECATED
GADGET_MESSAGE_END_OF_SCAN                            = 1003 # DEPRECATED
GADGET_MESSAGE_IMAGE_CPLX_FLOAT                       = 1004 # DEPRECATED
GADGET_MESSAGE_IMAGE_REAL_FLOAT                       = 1005 # DEPRECATED
GADGET_MESSAGE_IMAGE_REAL_USHORT                      = 1006 # DEPRECATED
GADGET_MESSAGE_EMPTY                                  = 1007 # DEPRECATED
GADGET_MESSAGE_ISMRMRD_ACQUISITION                    = 1008
GADGET_MESSAGE_ISMRMRD_IMAGE_CPLX_FLOAT               = 1009
GADGET_MESSAGE_ISMRMRD_IMAGE_REAL_FLOAT               = 1010
GADGET_MESSAGE_ISMRMRD_IMAGE_REAL_USHORT              = 1011
GADGET_MESSAGE_DICOM                                  = 1012
GADGET_MESSAGE_CLOUD_JOB                              = 1013
GADGET_MESSAGE_GADGETCLOUD_JOB                        = 1014
GADGET_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_CPLX_FLOAT     = 1015
GADGET_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_FLOAT     = 1016
GADGET_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_USHORT    = 1017
GADGET_MESSAGE_DICOM_WITHNAME                         = 1018
GADGET_MESSAGE_DEPENDENCY_QUERY                       = 1019
GADGET_MESSAGE_EXT_ID_MAX                             = 4096

MAX_BLOBS_LOG_10 = 6

GadgetMessageIdentifier = struct.Struct('<H')
SIZEOF_GADGET_MESSAGE_IDENTIFIER = len(GadgetMessageIdentifier.pack(0))
GadgetMessageConfigurationFile = struct.Struct('<1024s')
GadgetMessageScript = struct.Struct('<I')


def readsock(sock, bytecount):
    chunks = []
    curcount = 0
    while curcount < bytecount:
        chunk = sock.recv(min(bytecount - curcount, 4096))
        if chunk == '':
            raise RuntimeError("socket connection closed")
        chunks.append(chunk)
        curcount += len(chunk)
    return ''.join(chunks)


class GadgetronClientMessageReader(object):
    def read(self, sock):
        raise NotImplementedError("Must implement in derived class")

class GadgetronClientImageMessageReader(GadgetronClientMessageReader):
    def __init__(self, filename, groupname):
        self.filename = filename
        self.groupname = groupname
        self.dataset = None

    def read(self, sock):
        # read image header
        serialized_header = readsock(sock, ismrmrd_serial.SIZEOF_ISMRMRD_IMAGE_HEADER)
        head = ismrmrd_serial.deserialize_image_header(serialized_header)
        #data_bytes = (head.matrix_size[0] * head.matrix_size[1] * head.matrix_size[2] *
        #        head.channels * ismrmrd.sizeof_data_type(head.data_type))
        img = ismrmrd.Image()
        img.head = head

        # now the image's data should be a valid NumPy array of zeros
        # read image data
        dtype = img.data.dtype
        data_bytes = len(img.data.flat) * dtype.itemsize
        serialized_data = readsock(sock, data_bytes)
        img.data = np.fromstring(serialized_data, dtype=dtype)

        if not self.dataset:
            # open dataset
            self.dataset = ismrmrd.Dataset(self.filename, self.groupname)

        self.dataset.append_image("image_%d" % img.head.image_series_index, img)

class GadgetronClientAttribImageMessageReader(GadgetronClientMessageReader):
    def __init__(self, filename, groupname):
        self.filename = filename
        self.groupname = groupname
        self.dataset = None

    def read(self, sock):
        # read image header
        # read meta attributes
        # img.setAttributeString
        # read image data
        # open dataset
        # dset.appendImage
        pass

class GadgetronClientBlobMessageReader(GadgetronClientMessageReader):
    def __init__(self, prefix, suffix):
        self.prefix = prefix
        self.suffix = suffix
        self.num_calls = 0

    def read(self, sock):
        # read 32-bit unsigned integer x
        # read x # of bytes
        # create filename (<prefix>_<num_calls><suffix>)
        # save data to file
        # num_calls += 1
        pass

class GadgetronClientBlobAttribMessageReader(GadgetronClientMessageReader):
    def __init__(self, prefix, suffix):
        self.prefix = prefix
        self.suffix = suffix
        self.num_calls = 0

    def read(self, sock):
        # read 32-bit unsigned integer x
        # read x # of bytes (data)
        # read unsigned long long y
        # read y # of bytes (filename)
        # read unsigned long long z
        # read z # of bytes (meta attributes)
        # create filename (<prefix>_<num_calls><suffix>)
        # create filename_attrib (...xml)
        # save data to each file
        # num_calls += 1
        pass

class GadgetronClientConnector(object):
    def __init__(self, hostname=None, port=None):
        if hostname and port:
            self.connect(hostname, port)
        self.readers = {}
        self.writers = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reader_thread = None

    def connect(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.sock.connect((hostname, port))
        self.reader_thread = threading.Thread(name="GadgetronClientConnector read_task", target=self.read_task)
        self.reader_thread.start()

    def read_task(self):
        while True:
            msg = readsock(self.sock, SIZEOF_GADGET_MESSAGE_IDENTIFIER)
            kind = GadgetMessageIdentifier.unpack(msg)[0]

            if kind == GADGET_MESSAGE_CLOSE:
                break

            try:
                reader = self.readers[kind]
            except:
                print("Invalid message ID received: %d" % kind)
                raise
            reader.read(self.sock)

    def wait(self):
        self.reader_thread.join()

    def register_reader(self, kind, reader):
        self.readers[kind] = reader

    def register_writer(self, kind, writer):
        self.writers[kind] = writer

    def send_gadgetron_configuration_script(self, filename):
        pass

    def send_gadgetron_configuration_file(self, filename):
        msg = GadgetMessageIdentifier.pack(GADGET_MESSAGE_CONFIG_FILE)
        cfg = GadgetMessageConfigurationFile.pack(filename)
        self.sock.send(msg)
        self.sock.send(cfg)

    def send_gadgetron_parameters(self, xml):
        msg = GadgetMessageIdentifier.pack(GADGET_MESSAGE_PARAMETER_SCRIPT)
        script_len = GadgetMessageScript.pack(len(xml) + 1)
        xml_with_nul = '%s\0' % xml
        self.sock.send(msg)
        self.sock.send(script_len)
        self.sock.send(xml_with_nul)

    def send_gadgetron_close(self):
        msg = GadgetMessageIdentifier.pack(GADGET_MESSAGE_CLOSE)
        self.sock.send(msg)

    def send_ismrmrd_acquisition(self, acq):
        msg = GadgetMessageIdentifier.pack(GADGET_MESSAGE_ISMRMRD_ACQUISITION)
        buff = ismrmrd_serial.serialize_acquisition_header(acq.head)

        #trajectory_elements = acq.head.trajectory_dimensions * acq.head.number_of_samples
        #data_elements = 2 * acq.head.active_channels * acq.head.number_of_samples

        self.sock.send(msg)
        self.sock.send(buff)
        self.sock.send(acq.traj.tobytes())
        self.sock.send(acq.data.tobytes())

    def __del__(self):
        if self.sock:
            self.sock.close()
            self.sock = None
