package org.apache.flume.source.systemd.journal;

import com.bustleandflurry.systemd.journal.SystemdJournalLibrary;
import junit.framework.Assert;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.LoggerSink;
import org.bridj.Pointer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.mockito.internal.PowerMockitoCore;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bartlinga on 6/1/15.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SystemdJournalLibrary.class,Util.class})
public class TestJournalSource extends Assert {

/*
    @Mock
    Pointer<Byte> finalCursor;

    @Mock
    Pointer<Pointer<Byte>> cursorPointer;

    @Mock
    Pointer<Pointer<Byte>> existingCursor;
*/
    @Mock
    Util util;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }



    @InjectMocks
    JournalSource journalSource;


    @Test
    public void testBasic() throws Exception {

        Pointer<Pointer<SystemdJournalLibrary.sd_journal>> sdJournalPointer = null;
        Pointer<SystemdJournalLibrary.sd_journal> sdJournal = null;
        Pointer<Byte> cursor = null;
        Pointer<Byte> cursorPtr = Pointer.pointerToCString("00000");
        //Pointer<Byte> finalCursor = Pointer.pointerToCString("00000");

        PowerMockito.when(util.readPersistedCursor()).thenReturn(cursorPtr);

        Pointer<Pointer<Byte>> testPointer = Pointer.allocatePointer(Byte.class);
        testPointer.setCString("00000");


        PowerMockito.mockStatic(SystemdJournalLibrary.class);
        PowerMockito.when(SystemdJournalLibrary.sd_journal_open(sdJournalPointer, 0)).thenReturn(0);
        PowerMockito.when(SystemdJournalLibrary.sd_journal_seek_cursor(sdJournal, cursor)).thenReturn(0);
        PowerMockito.when(SystemdJournalLibrary.sd_journal_next(sdJournal)).thenReturn(0);
        PowerMockito.when(SystemdJournalLibrary.sd_journal_seek_tail(sdJournal)).thenReturn(0);
        PowerMockito.when(SystemdJournalLibrary.sd_journal_get_cursor(sdJournal, testPointer)).thenReturn(0);
/*
        PowerMockito.when(cursorPointer.get()).thenReturn(finalCursor);
        PowerMockito.when(existingCursor.get()).thenReturn(cursorPtr);
        PowerMockito.when(finalCursor.getCString()).thenReturn("00000");
*/







        // ----------------------------------------

        Context context = new Context();
        context.put("journalCursorFilePath", "/tmp/journalCursor.tmp");

        journalSource.configure(context);

        Map<String, String> channelContext = new HashMap<String, String>();
        channelContext.put("capacity", "1000000");
        channelContext.put("keep-alive", "0"); // for faster tests
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, new Context(channelContext));

        Sink sink = new LoggerSink();
        sink.setChannel(channel);
        sink.start();
        DefaultSinkProcessor proc = new DefaultSinkProcessor();
        proc.setSinks(Collections.singletonList(sink));
        SinkRunner sinkRunner = new SinkRunner(proc);
        sinkRunner.start();

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(Collections.singletonList(channel));
        ChannelProcessor chp = new ChannelProcessor(rcs);
        journalSource.setChannelProcessor(chp);
        journalSource.start();

        Thread.sleep(1000);

        /*
        try {
            journalSource.process();
            Assert.fail();
        } catch (EventDeliveryException e) {
            e.printStackTrace();
        }
        */


        journalSource.stop();
        sinkRunner.stop();
        sink.stop();
    }

}
