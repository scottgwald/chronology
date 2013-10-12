import urllib

def get_instance_id():
  return (urllib.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read())
