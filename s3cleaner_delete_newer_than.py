#!/usr/bin/env python

# python 2.7

#
# Find or delete files in S3 that were created after a given timestamp
#

from boto.s3.connection import S3Connection
import time
from optparse import OptionParser
import sys
import re
import io

def main(args):
  # Define script arguments
  parser = OptionParser()
  parser.add_option("--key", dest="key", metavar="KEY",
                    help="AWS Access Key")
  parser.add_option("--secret", dest="secret", metavar="SECRET",
                    help="AWS Access Secret Key")
  parser.add_option("--newerThan", dest="newerThan", metavar="UNIX_TIMESTAMP",
                    help="Find or delete all files that were created after this timestamp (absolute, not relative)")
  parser.add_option("--bucket", dest="bucket", metavar="BUCKET",
                    help="Search for keys in a specific bucket")
  parser.add_option("--dir", dest="dir", metavar="DIR",
                    help="Only consider keys in this DIR")
  parser.add_option("--delete", dest="delete", metavar="BOOLEAN", action="store_true",
                    default=False, help="Actually do a delete. If not specified, just list the keys found that match.")
  (config, args) = parser.parse_args(args)

  # Confirm script arguments
  config_ok = True
  for flag in ("key", "secret", "newerThan", "bucket", "dir"):
    if getattr(config, flag) is None:
      print >>sys.stderr, "Missing required flag: --%s" % flag
      config_ok = False
  if not config_ok:
    print >>sys.stderr, "Configuration is not ok, aborting..."
    return 1

  # Define log files
  filesToDeleteFileName = "files_newer_than_to_delete.txt"
  filesToKeepFileName = "files_newer_than_to_keep.txt"

  # Initialize log files
  filesNewerThanToDeleteLogFile = io.open(filesToDeleteFileName, mode="w", encoding="utf-8")
  filesNewerThanToKeepLogFile = io.open(filesToKeepFileName, mode="w", encoding="utf-8")

  # If these strings are found in the filename (and it was created after a
  # certain date/time), the file needs to be deleted
  fileNameParts = [
    "y4648e",
    "73x5ed",
    "cyf6x91y3g",
    "nh0y6d4",
    "dtp852",
    "6ls25j9b",
    "032iq",
    "6aka2o",
    "t45hu3pq",
    "2kmr06k8y"
  ]

  # print config.key
  # print config.secret

  # Get confirmation from the user before continuing
  print
  print "Going to go through s3 bucket:\n %s \n" % (config.bucket)
  print "Going to go through directory:\n %s \n" % (config.dir)
  print "Going to find/delete files that were created after:\n %s \n" % (config.newerThan)
  print "Going to find/delete files that contain an extension or prefix of any of the following:\n %s \n" % (', '.join(fileNameParts))
  print "Going to log list of files to delete to:\n %s \n" % (filesToDeleteFileName)
  print "Going to log list of files to keep to:\n %s \n" % (filesToKeepFileName)
  print "Goint to delete files?\n %s \n" % (config.delete)
  print
  raw_input("If everything looks good, press Enter to continue...")

  # Connect to s3
  s3Connection = S3Connection(config.key, config.secret)
  print "Successfully connected to s3..."

  # Ensure the arguments have the correct types
  config.newerThan = int(config.newerThan)

  # Connect to the correct bucket
  bucket = s3Connection.get_bucket(config.bucket)
  print "About to go through every file in bucket %s/%s..." % (bucket.name, config.dir)

  # Iterate over every single file in the specified directory
  for key in bucket.list(prefix=config.dir):
    # Convert the last_modified time to a unix timestamp
    mtime = time.mktime(time.strptime(key.last_modified.split(".")[0], "%Y-%m-%dT%H:%M:%S"))

    # If the file was created BEFORE the newerThan time, do not proceed
    if mtime < config.newerThan:
      continue

    # Check for an extension or prefix (contains) in the file name
    fileNameMatches = False
    for fileNamePart in fileNameParts:
      if fileNamePart in key.name:
        fileNameMatches = True
    if not fileNameMatches:
      filesNewerThanToKeepLogFile.write(u's3://{0}/{1}\n'.format(bucket.name, key.name))
      continue

    # If we've gotten this far, the file will be deleted
    filesNewerThanToDeleteLogFile.write(u's3://{0}/{1}\n'.format(bucket.name, key.name))

    if config.delete:
      # key.delete()
      continue

  filesNewerThanToKeepLogFile.close()
  filesNewerThanToDeleteLogFile.close()


if __name__ == '__main__':
  sys.exit(main(sys.argv))
