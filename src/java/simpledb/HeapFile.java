package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.getPageNumber();
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            fileInputStream.skip(pageNumber * BufferPool.getPageSize());
            fileInputStream.read(emptyPageData);
            return new HeapPage(new HeapPageId(pid.getTableId(), pageNumber), emptyPageData);
        } catch (IOException e) {
            throw new UnknownError(e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return ((int) file.length()) / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private final int tableId = getId();
            private int pgNo = -1;
            private Iterator<Tuple> pageIterator;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                pgNo = 0;
                // first page
                pageIterator = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo++), Permissions.READ_ONLY)).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // close ?
                // pgNo < 0
                if (pageIterator == null) {
                    return false;
                }

                // 0 <= pgNo < numPages()
                if (pageIterator.hasNext()) {
                    return true;
                }
                // next page
                if (pgNo < numPages()) {
                    pageIterator = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo++), Permissions.READ_ONLY)).iterator();
                    return pageIterator.hasNext();
                }

                // pgNo >= numPages()
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return pageIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pgNo = -1;
                pageIterator = null;
            }
        };
    }

}

