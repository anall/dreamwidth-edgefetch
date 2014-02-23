import sqlite3
import sys
import progressbar
import time
import struct
import collections
import array

accounts={}
pointer_count = 0

def midpoint(beg,end):
    return ( ( end - beg ) / 2 + beg )

def add_account(idx, username, ptr):
    if ( accounts.has_key(idx) ):
        accounts[idx] = (username, ptr, accounts[idx][2])
    else:
        accounts[idx] = (username, ptr, array.array('L'))

def add_account_pointer(idx,ptr):
    global pointer_count
    if ( accounts.has_key(idx) ):
        if ( accounts[idx][1] != -1 ):
            return accounts[idx][1]
        accounts[idx][2].append(ptr)
        pointer_count += 1
    else:
        accounts[idx] = (None, -1, array.array('L',[ptr]))
        pointer_count += 1
    return 0

def handle_edge_data_type(db, outfile, userid, type):
    ptr = outfile.tell()
    ct = 0
    outfile.write( struct.pack('<I',0) ) # Placeholder

    cur = db.cursor()
    for row in cur.execute('SELECT dest FROM edges AS e JOIN account AS a on dest=id WHERE src=? AND edge=?',(userid,type)):
        ct += 1
        outfile.write( struct.pack('<Q', add_account_pointer( row[0], outfile.tell() ) ) )

    retpos = outfile.tell()
    outfile.seek(ptr)

    if ct <= 0:
        return 0

    outfile.write( struct.pack('<I',ct) )
    outfile.seek(retpos)
    return ptr

def handle_edge_data(db, outfile, userid):
    types = ['trusted','trusted_by','watched','watched_by','member','member_of','maintainer']
    ptrs = [ handle_edge_data_type(db,outfile,userid,type) for type in types ]
    if len( filter( lambda x: x != 0, ptrs ) ) == 0:
        return 0

    ptr = outfile.tell()
    outfile.write( struct.pack("<7Q",*ptrs) )

    return ptr
 
def main(argv=sys.argv):
    if len(argv) != 3:
        print >> sys.stderr, "<program> <outfile> <database>"
        return -1

    outfile = open( argv[1], 'wb' )
    db = sqlite3.connect( argv[2] )

    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM account")
    count = cur.fetchone()[0]
    edge_count = 0
    
    start_time = time.time()
    outfile.write( struct.pack("<4sHIQQQIII",
        "----", 2,0, 0,0,0, 0,0,0) )
    widgets = ['%-25s ' % '--START--', progressbar.Percentage(), ' ',
                progressbar.Bar(),
           ' ', progressbar.ETA()]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=count).start()
    
    i = 0
    cur = db.cursor()
    for row in cur.execute("SELECT id,username,journaltype FROM account"):
        pbar.update( i )
        i += 1

        if ( row[0] < 0 ):
            continue

        edge_ptr = handle_edge_data( db, outfile, row[0] )

        if ( edge_ptr != 0 ):
            edge_count += 1

        widgets[0] = "%-25s " % ( row[1] )
        add_account( row[0], row[1], outfile.tell() )
        outfile.write( struct.pack("<QQQQcI", 0,0,edge_ptr,0,str(row[2]),row[0]) + str(row[1]) + "\0" )

    widgets[0] = "%-25s " % "Accounts written"
    pbar.finish()

    widgets = ['Creating account tree     ', progressbar.Percentage(), ' ',
                progressbar.Bar(),
           ' ', progressbar.ETA()]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(accounts) + 3).start()
    acct_sorted = sorted(accounts.values(),key=lambda x: x[0])
    pbar.update( 1 )


    i = 0
    root_idx = midpoint(0,len(acct_sorted)-1)
    root_ptr = acct_sorted[root_idx][1]

    outfile.seek(0)
    outfile.write( struct.pack("<4sHIQQQIII",
        "Pedg",2,start_time,
        root_ptr, # AccountData
        0, # InterestData
        0, # Extensions
        edge_count,len(accounts),0) )
    
    acct_todo = collections.deque( [ (0, len(acct_sorted) - 1) ] )
    while len(acct_todo):
        pbar.update( i + 2 )

        min_idx,max_idx = acct_todo.popleft()
        idx = midpoint(min_idx,max_idx)

        assert min_idx >= 0
        assert min_idx <= max_idx
        assert max_idx < len( acct_sorted )
        i += 1

        left_ptr = 0
        right_ptr = 0
        
        left_idx = midpoint(min_idx,idx-1)
        right_idx = midpoint(idx+1,max_idx)

        if ( idx > min_idx ):
            acct_todo.append( (min_idx,idx-1) )
            left_ptr = acct_sorted[left_idx][1]
        if ( idx < max_idx ):
            acct_todo.append( (idx+1,max_idx) )
            right_ptr = acct_sorted[right_idx][1]

        assert left_ptr != -1
        assert right_ptr != -1

        item_ptr = acct_sorted[idx][1]
        outfile.seek(item_ptr)
        outfile.write( struct.pack("<QQ",left_ptr,right_ptr) )
    pbar.finish()
    print len(accounts),i

    widgets = ['Finalizing pointers       ', progressbar.Percentage(), ' ',
                progressbar.Bar(),
           ' ', progressbar.ETA()]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=pointer_count).start()
    i = 0;
    for acct in accounts.values():
        ptr = acct[1]
        for wptr in acct[2]:
            pbar.update( i )
            i += 1

            print wptr, ptr
            outfile.seek( wptr )
            outfile.write( struct.pack("<Q",ptr) )
    pbar.finish()

    db.close()
    outfile.close()

if __name__ == "__main__":
    sys.exit(main())
