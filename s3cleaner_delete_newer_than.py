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
  # parser.add_option("--regex", dest="regex", metavar="REGEX",
  #                   help="Only consider keys matching this REGEX")
  parser.add_option("--log_file", dest="log_file", metavar="FILE",
                    help="The output will be written to this log FILE")
  parser.add_option("--delete", dest="delete", metavar="BOOLEAN", action="store_true",
                    default=False, help="Actually do a delete. If not specified, just list the keys found that match.")
  (config, args) = parser.parse_args(args)

  config_ok = True
  for flag in ("key", "secret", "newerThan", "bucket", "dir", "log_file"):
    if getattr(config, flag) is None:
      print >>sys.stderr, "Missing required flag: --%s" % flag
      config_ok = False

  if not config_ok:
    print >>sys.stderr, "Configuration is not ok, aborting..."
    return 1

  file = io.open(config.log_file + ".txt", mode="w", encoding="utf-8")

  # print config.key
  # print config.secret

  print
  print "Going to go through s3 bucket: %s" % (config.bucket)
  print "Going to go through directory: %s" % (config.dir)
  print "Going to find/delete files that were created after: %s" % (config.newerThan)
  # print "Going to find/delete files that match this regex: %s" % (config.regex)
  print "Going to log output to file: %s" % (config.log_file + '.txt')
  print "Goint to delete files? %s" % (config.delete)
  print

  raw_input("If everything looks good, press Enter to continue...")

  s3Connection = S3Connection(config.key, config.secret)

  print "Successfully connected to s3..."

  config.newerThan = int(config.newerThan)
  # config.regex = re.compile(config.regex)

  bucket = s3Connection.get_bucket(config.bucket)

  print "About to go through every file in bucket %s/%s..." % (bucket.name, config.dir)

  for key in bucket.list(prefix=config.dir):
    # # Skip, file does not match the pattern
    # if config.regex.search(key.name) is None:
    #   continue

    # Convert the last_modified time to a unix timestamp
    mtime = time.mktime(time.strptime(key.last_modified.split(".")[0], "%Y-%m-%dT%H:%M:%S"))

    # Skip, file was created BEFORE the newerThan time
    if mtime < config.newerThan:
      continue

    if config.delete:
      file.write(u'Deleting: s3://{0}/{1}\n'.format(bucket.name, key.name))
      file.write(u'  Key has age {0}, that is more recent than --newerThan {1}\n'.format(mtime, config.newerThan))
      # print "  Key matches pattern /%s/" % (config.regex.pattern)
      # key.delete()
    else:
      file.write(u'{0} --> s3://{1}/{2}\n'.format(mtime, bucket.name, key.name))

  file.close()


if __name__ == '__main__':
  sys.exit(main(sys.argv))
