import re
import os, popen2, time
from datetime import datetime
from optparse import make_option

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from django.core.management.base import BaseCommand, CommandError
from django.core.mail import EmailMessage
from django.conf import settings
from django.contrib.sites.models import Site


# Based on: http://code.google.com/p/django-backup/
# Based on: http://www.djangosnippets.org/snippets/823/
# Based on: http://www.yashh.com/blog/2008/sep/05/django-database-backup-view/
class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--email', '-m', default=None, dest='email',
            help='Sends email with attached dump file'),
        make_option('--compress', '-c', action='store_true', default=False, dest='compress',
            help='Compress SQL dump file using GZ'),
        make_option('--directory', '-d', action='append', default=[], dest='directories',
            help='Include Directories'),
        make_option('--zipencrypt', '-z', action='store_true', default=False,
            dest='zipencrypt', help='Compress and encrypt SQL dump file using zip'),
        make_option('--password', '-p', action='store', default="",
            dest='encrypt_password', help='zip encryption password. Or use settings.BACKUP_PASSWORD'),
        make_option('--backup_docs', '-b', action='store_true', default=False,
            dest='backup_docs', help='Backup your docs directory alongside the DB dump.'),
        make_option('--backup_dir', '-r', action='store', default=None,
            dest='backup_dir', help='Destination backup directory. Or use settings.BACKUP_DIR. Default is "backups"'),
        make_option('--s3', '-s', action='store_true', default=False, dest='s3',
            help='Upload new backups to Amazon S3 and remove old. Configure BACKUP_* in settings.py'),
    )
    help = "Backup database. Only Mysql, Postgresql and Sqlite engines are implemented"


    def handle(self, *args, **options):
        self.email = options.get('email')
        self.compress = options.get('compress')
        self.directories = options.get('directories')
        self.zipencrypt = options.get('zipencrypt')
        self.backup_docs = options.get('backup_docs')
        self.s3 = options.get('s3')
        if 'site' in settings.INSTALLED_APPS:
            self.current_site = Site.objects.get_current()
        else:
            self.current_site = ''
        if self.zipencrypt:
            self.encrypt_password = options.get('encrypt_password')
            if not self.encrypt_password:
                self.encrypt_password = getattr(settings, 'BACKUP_PASSWORD', '')

        if hasattr(settings, 'DATABASES'):
            #Support for changed database format
            self.engine = settings.DATABASES['default']['ENGINE']
            self.db = settings.DATABASES['default']['NAME']
            self.user = settings.DATABASES['default']['USER']
            self.passwd = settings.DATABASES['default']['PASSWORD']
            self.host = settings.DATABASES['default']['HOST']
            self.port = settings.DATABASES['default']['PORT']
        else:
            self.engine = settings.DATABASE_ENGINE
            self.db = settings.DATABASE_NAME
            self.user = settings.DATABASE_USER
            self.passwd = settings.DATABASE_PASSWORD
            self.host = settings.DATABASE_HOST
            self.port = settings.DATABASE_PORT
            
        #self.media_directory = settings.MEDIA_ROOT

        self.time_suffix = time.strftime('%Y%m%d-%H%M%S')
        
        backup_dir = options.get('backup_dir')
        if backup_dir is None:
            backup_dir = settings.BACKUP_DIR
        
        # odd logic
        if self.backup_docs or self.directories:
            backup_dir = os.path.join(backup_dir, self.time_suffix)

        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        dir_outfiles = []

        outfile = os.path.join(backup_dir, 'backup_%s.sql' % self.time_suffix)

        # Doing backup
        if 'mysql' in self.engine:
            print 'Doing Mysql backup to database %s into %s' % (self.db, outfile)
            self.do_mysql_backup(outfile)
        elif self.engine in ('postgresql_psycopg2', 'postgresql') or 'postgresql' in self.engine:
            print 'Doing Postgresql backup to database %s into %s' % (self.db, outfile)
            self.do_postgresql_backup(outfile)
        elif 'sqlite3' in self.engine:
            outfile = os.path.join(backup_dir, 'backup_%s.sqlite3' % self.time_suffix)
            print 'Doing sqlite backup to database %s into %s' % (self.db, outfile)
            self.do_sqlite_backup(outfile)
        else:
            raise CommandError('Backup in %s engine not implemented' % self.engine)

        # Compressing backup
        if self.compress:
            compressed_outfile = outfile + '.gz'
            print 'Compressing backup file %s to %s' % (outfile, compressed_outfile)
            self.do_compress(outfile, compressed_outfile)
            outfile = compressed_outfile
            dir_outfiles.append(outfile)

        #Zip & Encrypting backup
        if self.zipencrypt:
            zip_encrypted_outfile = outfile + ".zip"
            if self.encrypt_password:
                print 'Ziping and Encrypting backup file %s to %s' % (outfile,
                                                        zip_encrypted_outfile)
                self.do_encrypt(outfile, zip_encrypted_outfile, self.encrypt_password)
            else:
                print 'Ziping backup file %s to %s' % (outfile, zip_encrypted_outfile)
                #os.system('zip %s %s' % (zip_encrypted_outfile, outfile))
                self.do_zip(outfile, zip_encrypted_outfile)
                os.system('rm %s' % outfile)
            outfile = zip_encrypted_outfile
            dir_outfiles.append(outfile)


        """
        #Backup documents?
        if self.backup_docs:
            print "Backing up documents directory to %s from %s" % (backup_dir,
                                            self.media_directory)
            #dir_outfile = os.path.join(backup_dir, 'media_backup.tar.gz')
            dir_outfile = self.compress_dir(self.media_directory, backup_dir,
                              encrypt_password=self.encrypt_password)
            dir_outfiles.append(dir_outfile)
        """

        if self.backup_docs:
            self.directories = [settings.MEDIA_ROOT] + self.directories

        # Backup directoris
        for directory in self.directories:
            #dir_outfile = os.path.join(backup_dir, '%s_%s.tar.gz' % (
            #                os.path.basename(directory), self.time_suffix))
            #self.compress_dir(directory, dir_outfile, self.encrypt_password)
            dir_outfile = self.compress_dir(directory, backup_dir,
                              encrypt_password=self.encrypt_password)
            print("Backed up '%s' to '%s" % (directory, dir_outfile))
            dir_outfiles.append(dir_outfile)

        #for localfile in dir_outfiles:
        #    pass

        # Sending mail with backups
        if self.email:
            print "Sending e-mail with backups to '%s'" % self.email
            self.sendmail(settings.SERVER_EMAIL, [self.email], dir_outfiles)

        if self.s3:
            for localfile in dir_outfiles:
                print "Uploading {} to S3".format(localfile)
                self.upload_to_s3(localfile, settings.BACKUP_S3_BUCKET,
                    os.path.join(getattr(settings, 'BACKUP_S3_DIR', ''),
                                 self.time_suffix,
                                 os.path.basename(localfile)),
                    settings.BACKUP_AWS_ACCESS_KEY_ID,
                    settings.BACKUP_AWS_SECRET_ACCESS_KEY)

            self.s3_remove_old()

    def compress_dir(self, directory, backup_dir, encrypt_password=None):
        assert directory
        #assert outfile
        #outfile = os.path.join(backup_dir, 'media_backup.tar.gz')
        outfile = os.path.join(backup_dir,"{}_{}".format(
                        directory.replace("/","_"), self.time_suffix))
        if not encrypt_password:
            outfile += ".tar.gz"
            os.system('tar -czf %s %s' % (outfile, directory))
        else:
            outfile += ".zip"
            self.do_zip(directory, outfile, encrypt_password=encrypt_password,
                        recurse=True)
        return outfile

    def sendmail(self, address_from, addresses_to, attachements):
        subject = "Your DB-backup for %s %s" % (datetime.now().strftime("%d %b %Y"), self.current_site)
        body = "Timestamp of the backup is " + datetime.now().strftime("%d %b %Y")

        email = EmailMessage(subject, body, address_from, addresses_to)
        email.content_subtype = 'html'
        for attachement in attachements:
            email.attach_file(attachement)
        email.send()

    def do_compress(self, infile, outfile):
        os.system('gzip --stdout %s > %s' % (infile, outfile))
        os.system('rm %s' % infile)

    # TODO: get rid of shell parsing. use args
    def do_zip(self, infiles, outfile, encrypt_password=None, recurse=False):
        cmd = "zip"

        if recurse:
            cmd += ' -r'

        if encrypt_password:
            cmd += ' -P %s' % encrypt_password

        cmd += ' %s %s' % (outfile, infiles)

        os.system(cmd)

    def do_encrypt(self, infile, outfile, encrypt_password):
        #os.system('zip -P %s %s %s' % (self.encrypt_password, outfile, infile))
        self.do_zip(infile, outfile, encrypt_password)
        os.system('rm %s' % infile)
        
        #os.system('gpg --yes --passphrase %s -c %s' % (self.encrypt_password, infile))        
        #os.system('rm %s' % infile)

    def do_sqlite_backup(self, outfile):
        os.system('cp %s %s' % (self.db,outfile))

    def do_mysql_backup(self, outfile):
        args = []
        if self.user:
            args += ["--user=%s" % self.user]
        if self.passwd:
            args += ["--password=%s" % self.passwd]
        if self.host:
            args += ["--host=%s" % self.host]
        if self.port:
            args += ["--port=%s" % self.port]
        args += [self.db]

        os.system('mysqldump %s > %s' % (' '.join(args), outfile))

    def do_postgresql_backup(self, outfile):
        args = []
        if self.user:
            args += ["--username=%s" % self.user]
        if self.host:
            args += ["--host=%s" % self.host]
        if self.port:
            args += ["--port=%s" % self.port]
        if self.db:
            args += [self.db]
        if self.passwd:
            command = 'PGPASSWORD=%s pg_dump %s > %s' % (self.passwd, ' '.join(args), outfile)
        else:
            command = 'pg_dump %s -w > %s' % (' '.join(args), outfile)
        os.system(command)

    @staticmethod
    def upload_to_s3(localfile, bucket_name, key_name,
                     aws_access_key_id, aws_secret_access_key):
        conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        bucket = conn.get_bucket(bucket_name)
        k = Key(bucket, key_name)
        k.set_contents_from_filename(localfile)
        return k.etag

    @staticmethod
    def s3_bucket_ls_dir(bucket_name, dir,
                     aws_access_key_id, aws_secret_access_key):
        conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        bucket = conn.get_bucket(bucket_name)
        return bucket.list(prefix=dir)

    @staticmethod
    def s3_bucket_delete_keys(bucket_name, keys,
                     aws_access_key_id, aws_secret_access_key):
        conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        bucket = conn.get_bucket(bucket_name)
        res = bucket.delete_keys(keys)
        return res

    def backups_for_removing(self, files, keepnbackups):
        backups = []
        timestams = set()
        r = re.compile('.+_(20\d{6})-(\d{6})')
        for file in files:
            rm = r.match(file.key) 
            if rm is None:
                continue
            k = "".join(rm.groups())
            backups.append((k, file))
            timestams.add(k)
        old_timestamps = sorted(list(timestams))[0:-keepnbackups]
        oldbackups = map(lambda x:x[1],
                         filter(lambda x:x[0] in old_timestamps, backups))
        return oldbackups

    def s3_remove_old(self):

        s3_n_backups = getattr(settings, 'BACKUP_S3_KEEP_N_BACKUPS', 0)

        if not s3_n_backups:
            return
        
        files = self.s3_bucket_ls_dir(settings.BACKUP_S3_BUCKET,
            settings.BACKUP_S3_DIR, settings.BACKUP_AWS_ACCESS_KEY_ID,
            settings.BACKUP_AWS_SECRET_ACCESS_KEY)
        
        oldbackups = self.backups_for_removing(files, s3_n_backups)

        if oldbackups:
            print 'Removing {} old backup(s) on S3'.format(len(oldbackups))
            res = self.s3_bucket_delete_keys(settings.BACKUP_S3_BUCKET,
                oldbackups, settings.BACKUP_AWS_ACCESS_KEY_ID,
                settings.BACKUP_AWS_SECRET_ACCESS_KEY)
        #import pudb; pudb.set_trace()
        if res.errors:
            raise RuntimeError(" ".join(map(str, res.errors)))
