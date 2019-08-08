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
import pprint

def main(args):
  parser = OptionParser()
  parser.add_option("--key", dest="key", metavar="KEY",
                    help="AWS Access Key")
  parser.add_option("--secret", dest="secret", metavar="SECRET",
                    help="AWS Access Secret Key")
  parser.add_option("--deleteAfter", dest="deleteAfter", metavar="UNIX_TIMESTAMP",
                    help="Find or delete all files that were created after this timestamp (absolute, not relative)")
  parser.add_option("--regex", dest="regex", metavar="REGEX",
                    help="Only consider keys matching this REGEX")
  parser.add_option("--bucket", dest="bucket", metavar="BUCKET",
                    help="Search for keys in a specific bucket")
  parser.add_option("--delete", dest="delete", metavar="REGEX", action="store_true",
                    default=False, help="Actually do a delete. If not specified, just list the keys found that match.")
  (config, args) = parser.parse_args(args)

  config_ok = True
  for flag in ("key", "secret", "deleteAfter", "bucket"):
    if getattr(config, flag) is None:
      print >>sys.stderr, "Missing required flag: --%s" % flag
      config_ok = False

  if not config_ok:
    print >>sys.stderr, "Configuration is not ok, aborting..."
    return 1

  print config.key
  print config.secret

  print
  print "Going to go through s3 bucket: %s" % (config.bucket)
  print "Going to find/delete files that were created after: %s" % (config.deleteAfter)
  print "Going to find/delete files that match this regex: %s" % (config.regex)
  print "Goint to delete files? %s" % (config.delete)
  print

  raw_input("If everything looks good, press Enter to continue...")

  s3Connection = S3Connection(config.key, config.secret)

  print "Successfully connected to s3..."

  config.deleteAfter = int(config.deleteAfter)
  config.regex = re.compile(config.regex)

  bucket = s3Connection.get_bucket(config.bucket)

  print "About to go through every file in bucket %s..." % (bucket.name)

  for key in bucket.list():
    # Skip, file does not match the pattern
    if config.regex.search(key.name) is None:
      continue

    # Convert the last_modified time to a unix timestamp
    mtime = time.mktime(time.strptime(key.last_modified.split(".")[0], "%Y-%m-%dT%H:%M:%S"))

    # Skip, file was created BEFORE the deleteAfter time
    if mtime < config.deleteAfter:
      continue

    print "%s --> s3://%s/%s" % (mtime, bucket.name, key.name)

    if config.delete:
      print "Deleting: s3://%s/%s" % (bucket.name, key.name)
      print "  Key has age %s, that is more recent than --deleteAfter %s" % (mtime, config.deleteAfter)
      print "  Key matches pattern /%s/" % (config.regex.pattern)
      # key.delete()

if __name__ == '__main__':
  sys.exit(main(sys.argv))
