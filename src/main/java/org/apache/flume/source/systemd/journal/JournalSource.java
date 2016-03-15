package org.apache.flume.source.systemd.journal;

import com.bustleandflurry.systemd.journal.SystemdJournalLibrary;
import com.google.common.io.Files;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.bridj.Pointer;
import org.bridj.SizeT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Created by bartlinga on 5/30/15.
 */
public class JournalSource
        extends AbstractSource
        implements PollableSource, Configurable {


    private static final Logger LOGGER = LoggerFactory.getLogger(JournalSource.class);

    private Pointer<Pointer<SystemdJournalLibrary.sd_journal>> sdJournalPointer;
    private Pointer<SystemdJournalLibrary.sd_journal> sdJournal;
    private Pointer<Byte> existingCursor;

    private String journalCursorFilePath;

    private Util util;


    public void configure(Context context) {
        journalCursorFilePath = context.getString("journalCursorFilePath");

        LOGGER.info("Journal cursor file path is {}", journalCursorFilePath);
    }

    @Override
    public synchronized void start() {
        LOGGER.info("Starting journal source.");

        util = new Util(journalCursorFilePath);

        sdJournalPointer = Pointer.allocatePointer(SystemdJournalLibrary.sd_journal.class);
        int r = SystemdJournalLibrary.sd_journal_open(sdJournalPointer, 0);
        if(r < 0) {
            String errorMsg = "Unable to open systemd journal. " +
                    "Check that the Flume user has systemd journal read permissions. " +
                    "Linux error code: " + r;
            LOGGER.error(errorMsg);
            throw new FlumeException(errorMsg);
        }
        sdJournal = sdJournalPointer.getPointer(SystemdJournalLibrary.sd_journal.class);


        /*
        try {
            cursorFile = getPersistedCursorFile(journalCursorFilePath);
        } catch (IOException e) {
            String errorMsg = "Unable read persisted cursor file. " + journalCursorFilePath;
            LOGGER.error(errorMsg);
            throw new FlumeException(errorMsg);
        }
        */



        Pointer<Byte> persistedCursor = null;

        try {
            persistedCursor= util.readPersistedCursor();
        } catch (Exception e) {

        }

        if(persistedCursor != null) {
            LOGGER.info("persistedCursor is not null");
            SystemdJournalLibrary.sd_journal_seek_cursor(sdJournal, persistedCursor);
            SystemdJournalLibrary.sd_journal_next(sdJournal);

            existingCursor = persistedCursor;
        } else {
            LOGGER.info("persistedCursor is null");
            SystemdJournalLibrary.sd_journal_seek_tail(sdJournal);
            SystemdJournalLibrary.sd_journal_next(sdJournal);

            Pointer<Pointer<Byte>> cursorPointer = Pointer.allocatePointer(Byte.TYPE);
            SystemdJournalLibrary.sd_journal_get_cursor(sdJournal, cursorPointer);

            existingCursor = cursorPointer.get();
        }

        super.start();
    }

    @Override
    public synchronized void stop() {
        LOGGER.info("Journal source stopping.");
        //persistCursor();

        SystemdJournalLibrary.sd_journal_close(sdJournal);
        super.stop();
        LOGGER.info("Journal source stopped.");
    }

    /**
     *
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {
        LOGGER.info("process() called");
        SystemdJournalLibrary.sd_journal_next(sdJournal);

        Pointer<Pointer<Byte>> cursorPointer = Pointer.allocatePointer(Byte.TYPE);
        SystemdJournalLibrary.sd_journal_get_cursor(sdJournal, cursorPointer);

        Pointer<Byte> currentCursor = cursorPointer.get();

        if (existingCursor.getCString().equals(currentCursor.getCString())) {
            return Status.BACKOFF;
        } else {
            String journalEntry = readJournalEntry();
            Event event = EventBuilder.withBody(journalEntry, Charset.defaultCharset());
            getChannelProcessor().processEvent(event);
            existingCursor = currentCursor;
            //writePersistedCursor(currentCursor.getCString());
        }
        return Status.READY;
    }

    /**
     * Read the current journal entry
     * @return
     */
    private String readJournalEntry() {
        Pointer<Pointer<?>> dataPointer = Pointer.allocatePointer();
        Pointer<SizeT> sizePointer = Pointer.allocateSizeT();

        ArrayList<String> journalEntryList = new ArrayList<String>();

        while(SystemdJournalLibrary.sd_journal_enumerate_data(sdJournal, dataPointer, sizePointer) > 0) {
            Pointer<Byte> data = dataPointer.as(Byte.class);
            journalEntryList.add(data.getPointer(Byte.class).getCString());
        }

        SystemdJournalLibrary.sd_journal_restart_data(sdJournal);
        String journalEntry = journalEntryList.toString().replace("[", "").replace("]", "")
                .replace(", ", ",");
        return journalEntry;
    }




}
