# MediaGrabber
MediaGrabber is a commandline tool intended to run as a scheduled task which collects photos/videos from different 
source directories into a target directory, renames them by their EXIF create date and stores them in a folder structure
based on their creation dates - while keeping records of sources and target files in an index file (database) to
speed up things for the following runs.

## Usage / Command line Options
The following commandline options and flags control the tool's behavior 

Option | Long | Value | Description
:---: | :--- | :--- | :---
-m | --mode | `import` | import files from source dirs to target dir and index
| " |   "    | `index` | validate/update target index
| " |    "    | `reset` | reset sources: remove all source infos (but keep target index)
-s | --sourcedirs | *list of source directories* | list of directory paths to import from (use "" for paths with spaces), separate different paths by spaces
-t | --targetdir | *target directory* | directory to import to and to store the index file
-e | --extensions | *list of file extensions, separated by spaces* | list of file extensions to import to target (default: jpg)
-i | --ignore-dirs | *list of patterns, separated by spaces* | exclude patterns to filter subdirectories which should not be imported
-r | --remove-sources | `none` | move (instead of copy) files from source to target
-p | --probe | `none` | do no touch files - preview only
-q | --quiet | `none` | no processing output to console
-v | --verbose | `none` | output verbose processing information to console
-l | --logfile | *(optional: logfile)* | write logfile (optional: specify logfile name)
-d | --debug | `none` | create detailed debug logfile


## Examples

* Import files from sources to target:
    ```
    python3 ./mediagrabber.py
    -m import 
    -s "C:\Users\Thomas\Dropbox\Camera-Uploads" "C:\Users\Thomas\Other Photos"
    -t "C:\Users\Thomas\Photos\My awesome photo collection"
    -e jpg jpeg
    -i "Camera-Uploads\old images" archive .svn
    ```
    

* Run import job as scheduled task on a Synology NAS:
    ```
    export LANG=en_US.UTF-8
    python3 /volume1/homes/admin/scripts/python/mediagrabber/mediagrabber.py
    -m import 
    -s /volume1/homes/thomas/Dropbox/Camera-Uploads 
    -t /volume1/photo
    -e jpg jpeg
    -i @eaDir \.svn __ 
    -q >> /volume1/homes/admin/scripts/python/mediagrabber/photograbber.out 2>&1
    ```

* Rebuild index of target files (e.g. after having cleaned up some of those real bad photos =) :
     ```
    python3 ./mediagrabber.py
    -m index
    -t /volume1/photo
    -e jpg jpeg
    -i @eaDir \.svn __
    ```
## How the Code works
When starting the tool for the first time, you would typically do this in `import` mode, specifiying
 * the target directory
 * the source directories to import from
 * the file extensions to import
 
For each matching file in the source directories, the tool then gets the creation timestamp from the EXIF metadata of
the file and calculates its MD5 hash - this info is recorded in the index database (target) along with the information
on where the file was imported from (sources) and used in later runs to speed up things.

The file is then renamed according the creation timestamp of its content (YYYY-MM-DD HH:mm:ss) and copied/moved into the 
target directory.
  
When importing, the tool does the following:
 1. Check if the source is known (already in db?) => skip, if yes
 2. Check if the source file is already in target (name, size, md5) => add to source list, if yes
 3. If the file is unknown, add source and target records and copy/move the file into the target structure

In the target directory, the files are stored in the following structure:
    
    2017 
 
       + 2017-05
       
            + 2017-05-29
                 + 2017-05-29 09.15.05.jpg
                 + 2017-05-29 09.19.23.jpg
                 
            + 2017-05-30
                 + 2017-05-30 15.15.05.jpg
 
       + 2017-06
 
            + 2017-06-01
                 + 2017-06-01 08.19.05.jpg
     etc.

When the tool is run again, most of the sources will be known and the tool will only try to insert files into the target
structure which were added since the last run.

 
 ## Acknowledgments

This only works thanks to 
  * the superb [ExifTool by Phil Harvey](http://www.sno.phy.queensu.ca/~phil/exiftool/) and
  * the excellent Python wrapper [pyexiftool by Sven Marnach](https://github.com/smarnach/pyexiftool)
  * the [md5 code for large files](http://stackoverflow.com/a/17782753) by Bastien Semene
