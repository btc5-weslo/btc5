import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("BTC5_DB", os.path.join(tempfile.mkdtemp(), "test.db"))   # tests never touch the live database
