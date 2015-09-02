package com.netflix.eureka2.server.interest;

import com.netflix.eureka2.channel.ChannelFactory;
import com.netflix.eureka2.channel.InterestChannel;
import com.netflix.eureka2.client.channel.ClientInterestChannel;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.ChangeNotification.Kind;
import com.netflix.eureka2.interests.IndexRegistryImpl;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.interests.SourcedChangeNotification;
import com.netflix.eureka2.interests.SourcedStreamStateNotification;
import com.netflix.eureka2.interests.StreamStateNotification;
import com.netflix.eureka2.registry.EurekaRegistry;
import com.netflix.eureka2.registry.EurekaRegistryImpl;
import com.netflix.eureka2.registry.Source;
import com.netflix.eureka2.registry.Source.Origin;
import com.netflix.eureka2.registry.instance.InstanceInfo;
import com.netflix.eureka2.rx.ExtTestSubscriber;
import com.netflix.eureka2.testkit.data.builder.SampleInstanceInfo;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static com.netflix.eureka2.metric.EurekaRegistryMetricFactory.registryMetrics;
import static com.netflix.eureka2.testkit.junit.EurekaMatchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Tomasz Bak
 */
public class FullFetchInterestClientTest {

    private static final ChangeNotification<InstanceInfo> ADD_INSTANCE_1 = new ChangeNotification<>(Kind.Add, SampleInstanceInfo.EurekaWriteServer.build());
    private static final ChangeNotification<InstanceInfo> ADD_INSTANCE_2 = new ChangeNotification<>(Kind.Add, SampleInstanceInfo.EurekaWriteServer.build());
    private static final ChangeNotification<InstanceInfo> ADD_ANOTHER_VIP = new ChangeNotification<>(Kind.Add, SampleInstanceInfo.EurekaReadServer.build());

    private static final Interest<InstanceInfo> INTEREST = Interests.forVips(ADD_INSTANCE_1.getData().getVipAddress());
    private static final Source SOURCE = new Source(Origin.INTERESTED, "test");

    private static final StreamStateNotification<InstanceInfo> BUFFER_BEGIN = SourcedStreamStateNotification.bufferStartNotification(Interests.forFullRegistry(), SOURCE);
    private static final StreamStateNotification<InstanceInfo> BUFFER_END = SourcedStreamStateNotification.bufferEndNotification(Interests.forFullRegistry(), SOURCE);

    private final TestScheduler testScheduler = Schedulers.test();

    private final EurekaRegistry<InstanceInfo> registry = new EurekaRegistryImpl(
            new IndexRegistryImpl<InstanceInfo>(), registryMetrics(), testScheduler);

    private final ChannelFactory<InterestChannel> channelFactory = mock(ChannelFactory.class);

    private final ExtTestSubscriber<ChangeNotification<InstanceInfo>> testSubscriber = new ExtTestSubscriber<>();
    private final ClientInterestChannel interestChannel = mock(ClientInterestChannel.class);

    @Before
    public void setUp() throws Exception {
        when(channelFactory.newChannel()).thenReturn(interestChannel);
    }

    @Test
    public void testChannelHasSingleFullRegistryFetchSubscription() throws Exception {
        new FullFetchInterestClient(registry, channelFactory).forInterest(INTEREST).subscribe(testSubscriber);
        testScheduler.triggerActions();

        verify(interestChannel, times(1)).change(Interests.forFullRegistry());
        verify(interestChannel, times(0)).change(INTEREST);
    }

    @Test
    public void testBufferMarkersFromTheChannelArePropagatedToSubscriber() throws Exception {
        new FullFetchInterestClient(registry, channelFactory).forInterest(INTEREST).subscribe(testSubscriber);

        PublishSubject<ChangeNotification<InstanceInfo>> interestStream = PublishSubject.create();
        registry.connect(SOURCE, interestStream).subscribe();
        testScheduler.triggerActions();

        interestStream.onNext(BUFFER_BEGIN);
        interestStream.onNext(new SourcedChangeNotification<>(ADD_INSTANCE_1, SOURCE));
        interestStream.onNext(new SourcedChangeNotification<>(ADD_INSTANCE_2, SOURCE));
        interestStream.onNext(new SourcedChangeNotification<>(ADD_ANOTHER_VIP, SOURCE));
        interestStream.onNext(BUFFER_END);
        testScheduler.triggerActions();

        assertThat(testSubscriber.takeNext(), is((bufferStartNotification())));
        assertThat(testSubscriber.takeNext(), is((bufferEndNotification())));
        assertThat(testSubscriber.takeNext(), is((bufferStartNotification())));
        assertThat(testSubscriber.takeNext().getData(), is(equalTo(ADD_INSTANCE_1.getData())));
        assertThat(testSubscriber.takeNext().getData(), is(equalTo(ADD_INSTANCE_2.getData())));
        assertThat(testSubscriber.takeNext(), is((bufferEndNotification())));
    }
}