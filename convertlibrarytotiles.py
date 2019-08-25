#!/usr/bin/env python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# version 1.1.2

# This script does the following:
# 1. Extracts ESPA-processed Landsat imagery data from tar.gz files
# 2. Virtually stacks surface reflectance (SR) and brightness temperature (BT) bands. 
# 3. Converts SR, BT, and Fmask data from UTM to the local projection.
# 4. Calculates NDVI and EVI for clear land pixels
# 5. Archives tar.gz files after use

import os, sys, glob, argparse#, ieo, datetime, shutil
#from osgeo import ogr

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
    ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()
        
## main
parser = argparse.ArgumentParser('This script imports ESPA-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
parser.add_argument('-i','--indir', default = ieo.ingestdir, type = str, help = 'Input directory to search for files. This will be overridden if --infile is set.')
parser.add_argument('-if','--infile', type = str, help = 'Input file. This must be contain the full path and filename.')
parser.add_argument('-f','--fmaskdir', type = str, default = ieo.fmaskdir, help = 'Directory containing FMask cloud masks in local projection.')
parser.add_argument('-q','--pixelqadir', type = str, default = ieo.pixelqadir, help = 'Directory containing Landsat pixel QA layers in local projection.')
parser.add_argument('-s', '--srdir', type = str, default = ieo.srdir, help = 'Surface reflectance output directory')
parser.add_argument('-b', '--btdir', type = str, default = ieo.btdir, help = 'Brightness temperature output directory')
parser.add_argument('-n', '--ndvidir', type = str, default = ieo.ndvidir, help = 'NDVI output directory')
parser.add_argument('-e', '--evidir', type = str, default = ieo.evidir, help = 'EVI output directory')
parser.add_argument('-a', '--archdir', type = str, default = ieo.archdir, help = 'Original data archive directory')
parser.add_argument('--overwrite', type = bool, default = False, help = 'Overwrite existing files rather than update.')
parser.add_argument('-d', '--delete', type = bool, default = False, help = 'Delete input files after execution.')
parser.add_argument('-m', '--move', type = bool, default = False, help = 'Move input files to archive after execution.')
parser.add_argument('-r','--remove', type = bool, default = False, help = 'Remove temporary files after ingest.')
parser.add_argument('-o','--outdir', type = str, default = None, help = 'Output directory for tiles.')
parser.add_argument('-v','--vrt', type = bool, default = False, help = 'Use VRTs rather than input files.')
parser.add_argument('-k','--skipqa', action = 'store_true', help = 'Skip conversion of Pixel QA and Fmask files.')
parser.add_argument('-nu','--noupdate', action = 'store_true', help = 'Do not update tiles with new data.')
args = parser.parse_args()

dirs = [args.pixelqadir, args.fmaskdir, args.srdir, args.btdir, args.ndvidir, args.evidir]

if args.outdir:
    if not os.path.isdir(args.outdir):
        os.mkdir(args.outdir)

rastertypes =['pixel_qa', 'Fmask', 'ref', 'BT', 'NDVI', 'EVI']

for d in dirs:
    dn = dirs.index(d)
    if args.skipqa and d in [args.pixelqadir, args.fmaskdir]:
        continue
    if args.outdir:
        outdir = os.path.join(args.outdir, os.path.basename(d))
        if not os.path.isdir(outdir):
            os.mkdir(outdir)
    else: 
        outdir = d
    if args.vrt:
        if not d.endswith('vrt') and not d in [args.pixelqadir, args.fmaskdir]:
            d = os.path.join(d, 'vrt')
        flist = glob.glob(os.path.join(d, 'L*.vrt'))
    else:
        flist = glob.glob(os.path.join(d, 'L*.dat'))
    if len(flist) > 0:
        print('Now converting {} scenes to tiles from: \nCreating tiles in: {}'.format(len(flist), d, outdir))
        for f in flist:
            print('Converting: {} ({}/{})'.format(os.path.basename(f), flist.index(f) + 1, len(flist)))
#            if dn in [2, 3]:
#                rastertype = rastertypes[dn][os.path.basename(f)[2:3]]
#            else:
            rastertype = rastertypes[dn]
            if dn < 2:
                pixelqa = False
            else:
                pixelqa = True
            try:
                ieo.converttotiles(f, outdir, rastertype, pixelqa = pixelqa, overwrite = args.overwrite, noupdate = args.noupdate)
            except Exception as e:
                ieo.logerror(f, e)
                print('ERROR with file {}:\n{}'.format(f,e))
    
        