package org.apache.flume.source.systemd.journal;

import com.bustleandflurry.systemd.journal.SystemdJournalLibrary;
import com.google.common.io.Files;
import org.apache.flume.FlumeException;
import org.bridj.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by bartlinga on 6/3/15.
 */
public class Util implements JournalSourceUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JournalSource.class);
    private Pointer<Byte> finalCursor;
    private String journalCursorFilePath;
    private File cursorFile;

    private Pointer<SystemdJournalLibrary.sd_journal> sdJournal;

    public Util(String journalCursorFilePath) {
        this.journalCursorFilePath = journalCursorFilePath;
        cursorFile = new File(journalCursorFilePath);
    }

    private Boolean persistCursor() {
        Pointer<Pointer<Byte>> cursorPointer = Pointer.allocatePointer(Byte.TYPE);
        SystemdJournalLibrary.sd_journal_get_cursor(sdJournal, cursorPointer);

        finalCursor = cursorPointer.get();
        String result = finalCursor.getCString();

        writePersistedCursor(result);
        return true;
    }

    /**
     *
     * @return
     */
    public Pointer<Byte> readPersistedCursor() {
        String cursor = null;

        try {
            cursor = Files.toString(cursorFile, Charset.defaultCharset());
            LOGGER.info("Cursor {} ", cursor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(cursor.length() <= 0) {
            return null;
        }
        Pointer<Byte> cursorPtr = Pointer.pointerToCString(cursor);
        return cursorPtr;
    }

    /**
     *
     * @param cursor
     */
    private void writePersistedCursor(String cursor) {
        try {
            Files.write(cursor, cursorFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new FlumeException(e.getMessage());
        }
    }

    /**
     *
     * @param journalCursorFilePath
     * @return
     * @throws IOException
     */
    private File getPersistedCursorFile(String journalCursorFilePath) throws IOException {
        File cursorFile = new File(journalCursorFilePath);
        if(!cursorFile.createNewFile()) {
            if(cursorFile.canWrite()) {
                return cursorFile;
            }
        }
        return cursorFile;
    }


}
