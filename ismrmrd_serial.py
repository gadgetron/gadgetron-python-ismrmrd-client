import ismrmrd
import struct

def serialize_acquisition_header(head):
    return _AcquisitionHeaderRepr.pack(
            head.version,
            head.flags,
            head.measurement_uid,
            head.scan_counter,
            head.acquisition_time_stamp,
            head.physiology_time_stamp[0], head.physiology_time_stamp[0], head.physiology_time_stamp[0],
            head.number_of_samples,
            head.available_channels,
            head.active_channels,
            head.channel_mask[0], head.channel_mask[1], head.channel_mask[2], head.channel_mask[3],
            head.channel_mask[4], head.channel_mask[5], head.channel_mask[6], head.channel_mask[7],
            head.channel_mask[8], head.channel_mask[9], head.channel_mask[10], head.channel_mask[11],
            head.channel_mask[12], head.channel_mask[13], head.channel_mask[14], head.channel_mask[15],
            head.discard_pre,
            head.discard_post,
            head.center_sample,
            head.encoding_space_ref,
            head.trajectory_dimensions,
            head.sample_time_us,
            head.position[0], head.position[0], head.position[0],
            head.read_dir[0], head.read_dir[1], head.read_dir[2],
            head.phase_dir[0], head.phase_dir[1], head.phase_dir[2],
            head.slice_dir[0], head.slice_dir[1], head.slice_dir[2],
            head.patient_table_position[0], head.patient_table_position[1], head.patient_table_position[2],
            head.idx.kspace_encode_step_1,
            head.idx.kspace_encode_step_2,
            head.idx.average,
            head.idx.slice,
            head.idx.contrast,
            head.idx.phase,
            head.idx.repetition,
            head.idx.set,
            head.idx.segment,
            head.idx.user[0], head.idx.user[1], head.idx.user[2], head.idx.user[3],
            head.idx.user[4], head.idx.user[5], head.idx.user[6], head.idx.user[7],
            head.user_int[0], head.user_int[1], head.user_int[2], head.user_int[3],
            head.user_int[4], head.user_int[5], head.user_int[6], head.user_int[7],
            head.user_float[0], head.user_float[1], head.user_float[2], head.user_float[3],
            head.user_float[4], head.user_float[5], head.user_float[6], head.user_float[7])

def deserialize_acquisition_header(serialized):
    fields = _AcquisitionHeaderRepr.unpack(serialized)
    head = ismrmrd.AcquisitionHeader()
    head.version = fields[0]
    head.flags = fields[1]
    head.measurement_uid = fields[2]
    head.scan_counter = fields[3]
    head.acquisition_time_stamp = fields[4]
    head.physiology_time_stamp = fields[5:8]
    head.number_of_samples = fields[9]
    head.available_channels = fields[10]
    head.active_channels = fields[11]
    head.channel_mask = fields[12:28]
    head.discard_pre = fields[29]
    head.discard_post = fields[30]
    head.center_sample = fields[31]
    head.encoding_space_ref = fields[32]
    head.trajectory_dimensions = fields[33]
    head.sample_time_us = fields[34]
    head.position = fields[35:38]
    head.read_dir = fields[38:41]
    head.phase_dir = fields[41:44]
    head.slice_dir = fields[44:47]
    head.patient_table_position = fields[47:50]
    head.idx.kspace_encode_step_1 = fields[51]
    head.idx.kspace_encode_step_2 = fields[52]
    head.idx.average = fields[53]
    head.idx.slice = fields[54]
    head.idx.contrast = fields[55]
    head.idx.phase = fields[56]
    head.idx.repetition = fields[57]
    head.idx.set = fields[58]
    head.idx.segment = fields[59]
    head.idx.user = fields[60:68]
    head.user_int = fields[68:76]
    head.user_float = fields[76:84]
    return head


def serialize_image_header(head):
    return _ImageHeaderRepr.pack(
            head.version,
            head.data_type,
            head.flags,
            head.measurement_uid,
            head.matrix_size[0], head.matrix_size[1], head.matrix_size[2],
            head.field_of_view[0], head.field_of_view[1], head.field_of_view[2],
            head.channels,
            head.position[0], head.position[0], head.position[0],
            head.read_dir[0], head.read_dir[1], head.read_dir[2],
            head.phase_dir[0], head.phase_dir[1], head.phase_dir[2],
            head.slice_dir[0], head.slice_dir[1], head.slice_dir[2],
            head.patient_table_position[0], head.patient_table_position[1], head.patient_table_position[2],
            head.average,
            head.slice,
            head.contrast,
            head.phase,
            head.repetition,
            head.set,
            head.acquisition_time_stamp,
            head.physiology_time_stamp[0], head.physiology_time_stamp[1], head.physiology_time_stamp[2],
            head.image_type,
            head.image_index,
            head.image_series_index,
            head.user_int[0], head.user_int[1], head.user_int[2], head.user_int[3],
            head.user_int[4], head.user_int[5], head.user_int[6], head.user_int[7],
            head.user_float[0], head.user_float[1], head.user_float[2], head.user_float[3],
            head.user_float[4], head.user_float[5], head.user_float[6], head.user_float[7],
            head.attribute_string_len)

def deserialize_image_header(serialized):
    fields = _ImageHeaderRepr.unpack(serialized)
    head = ismrmrd.ImageHeader()
    head.version = fields[0]
    head.data_type = fields[1]
    head.flags = fields[2]
    head.measurement_uid = fields[3]
    head.matrix_size = fields[4:7]
    head.field_of_view = fields[7:10]
    head.channels = fields[10]
    head.position = fields[11:14]
    head.read_dir = fields[14:17]
    head.phase_dir = fields[17:20]
    head.slice_dir = fields[20:23]
    head.patient_table_position = fields[23:26]
    head.average = fields[26]
    head.slice = fields[27]
    head.contrast = fields[28]
    head.phase = fields[29]
    head.repetition = fields[30]
    head.set = fields[31]
    head.acquisition_time_stamp = fields[32]
    head.physiology_time_stamp = fields[33:36]
    head.image_type = fields[36]
    head.image_index = fields[37]
    head.image_series_index = fields[38]
    head.user_int = fields[39:47]
    head.user_float = fields[47:55]
    head.attribute_string_len = fields[55]
    return head


_encoding_counters_repr = '9H %dH' % ismrmrd.USER_INTS
_acquisition_header_repr = 'HQIII %dI HHH %dQ HHHHHf3f3f3f3f3f %s %di %df' % (
        ismrmrd.PHYS_STAMPS, ismrmrd.CHANNEL_MASKS, _encoding_counters_repr,
        ismrmrd.USER_INTS, ismrmrd.USER_FLOATS)
_image_header_repr = 'HHQI3H3fH3f3f3f3f3fHHHHHHI %dI HHH %di %df I' % (
        ismrmrd.PHYS_STAMPS, ismrmrd.USER_INTS, ismrmrd.USER_FLOATS)
_AcquisitionHeaderRepr = struct.Struct('<%s' % _acquisition_header_repr)
_ImageHeaderRepr = struct.Struct('<%s' % _image_header_repr)

SIZEOF_ISMRMRD_ACQUISITION_HEADER = len(serialize_acquisition_header(ismrmrd.AcquisitionHeader()))
SIZEOF_ISMRMRD_IMAGE_HEADER = len(serialize_image_header(ismrmrd.ImageHeader()))
