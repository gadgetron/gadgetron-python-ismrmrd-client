#!/usr/bin/env python

import gtconnector as gt
import ismrmrd

import argparse
import datetime
import logging

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='send acquisitions to Gadgetron',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('filename', help='Input file')
    parser.add_argument('-a', '--address', help='Address (hostname) of Gadgetron Host')
    parser.add_argument('-p', '--port', type=int, help='Port')
    parser.add_argument('-o', '--outfile', help='Output file')
    parser.add_argument('-g', '--in-group', help='Input data group')
    parser.add_argument('-G', '--out-group', help='Output group name')
    parser.add_argument('-c', '--config', help='Remote configuration file')
    parser.add_argument('-C', '--config-local', help='Local configuration file')
    parser.add_argument('-l', '--loops', type=int, help='Loops')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')

    parser.set_defaults(address='localhost', port='9002', outfile='out.h5',
            in_group='/dataset', out_group=str(datetime.datetime.now()),
            config='default.xml', loops=1)

    args = parser.parse_args()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)

    logger.debug("Instantiating Connector")
    con = gt.Connector()

    con.register_reader(gt.GADGET_MESSAGE_ISMRMRD_IMAGE_REAL_USHORT,
            gt.ImageMessageReader(args.outfile, args.out_group))
    con.register_reader(gt.GADGET_MESSAGE_ISMRMRD_IMAGE_REAL_FLOAT,
            gt.ImageMessageReader(args.outfile, args.out_group))
    con.register_reader(gt.GADGET_MESSAGE_ISMRMRD_IMAGE_CPLX_FLOAT,
            gt.ImageMessageReader(args.outfile, args.out_group))

    # Image with attributes
    con.register_reader(gt.GADGET_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_USHORT,
            gt.ImageAttribMessageReader(args.outfile, args.out_group))
    con.register_reader(gt.GADGET_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_FLOAT,
            gt.ImageAttribMessageReader(args.outfile, args.out_group))
    con.register_reader(gt.GADGET_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_CPLX_FLOAT,
            gt.ImageAttribMessageReader(args.outfile, args.out_group))

    # DICOM
    con.register_reader(gt.GADGET_MESSAGE_DICOM,
            gt.BlobMessageReader(args.out_group, 'dcm'))
    con.register_reader(gt.GADGET_MESSAGE_DICOM_WITHNAME,
            gt.BlobAttribMessageReader('', 'dcm'))

    logger.debug("Connecting to Gadgetron @ %s:%d" % (args.address, args.port))
    con.connect(args.address, args.port)

    if (args.config_local):
        logger.debug("Sending gadgetron configuration script %s", args.config_local)
        con.send_gadgetron_configuration_script(args.config_local)
    else:
        logger.debug("Sending gadgetron configuration filename %s", args.config)
        con.send_gadgetron_configuration_file(args.config)

    dset = ismrmrd.Dataset(args.filename, args.in_group, False)
    if not dset:
        parser.error("Not a valid dataset: %s" % args.filename)

    xml_config = dset.read_header()
    con.send_gadgetron_parameters(xml_config)

    for idx in range(dset.number_of_acquisitions):
        logger.debug("Sending acquisition %d", idx)
        acq = dset.read_acquisition(idx)
        try:
            con.send_ismrmrd_acquisition(acq)
        except:
            logger.error('Failed to send acquisition %d' % idx)
            return

    logger.debug("Sending close message to Gadgetron")
    con.send_gadgetron_close()
    con.wait()


if __name__ == '__main__':
    main()
