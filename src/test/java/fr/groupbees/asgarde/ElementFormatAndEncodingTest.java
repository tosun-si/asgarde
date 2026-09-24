package fr.groupbees.asgarde;

import avro.generated.AvroTest;
import fr.groupbees.asgarde.transforms.BaseElementFn;
import fr.groupbees.asgarde.transforms.FilterFn;
import fr.groupbees.asgarde.transforms.FlatMapElementFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contains the tests of the input element format ({@code withInputElementToString}) and of the encoded elements
 * ({@code withEncodedElements}) of the composers.
 */
public class ElementFormatAndEncodingTest implements Serializable {

    private static final String FAIL = "Fail";

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void givenInputElementToString_whenFailuresInAllKindsOfSteps_thenInputElementsWithTheGivenFormat() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("bad"));

        // When.
        final CollectionComposer<String> composer = CollectionComposer.of(words)
                .withInputElementToString(element -> "formatted " + element);

        final Result<PCollection<String>, Failure> mapElements = composer
                .apply("MapElements", MapElements.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();
        final Result<PCollection<String>, Failure> flatMapElements = composer
                .apply("FlatMapElements", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String word) -> Collections.singletonList(failIfBad(word))))
                .getResult();
        final Result<PCollection<String>, Failure> mapElementFn = composer
                .apply("MapElementFn", MapElementFn.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();
        final Result<PCollection<String>, Failure> filterFn = composer
                .apply("FilterFn", FilterFn.by(word -> !failIfBad(word).isEmpty()))
                .getResult();
        final Result<PCollection<String>, Failure> customDoFn = composer
                .apply("Custom DoFn", new FailIfBadFn())
                .getResult();
        final Result<PCollection<String>, Failure> originFn = composer
                .withOriginElement(word -> "origin " + word)
                .apply("Origin MapElementFn", MapElementFn.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();

        // Then.
        PAssert.that("MapElements", toInputElements(mapElements.failures(), "1")).containsInAnyOrder("formatted bad");
        PAssert.that("FlatMapElements", toInputElements(flatMapElements.failures(), "2")).containsInAnyOrder("formatted bad");
        PAssert.that("MapElementFn", toInputElements(mapElementFn.failures(), "3")).containsInAnyOrder("formatted bad");
        PAssert.that("FilterFn", toInputElements(filterFn.failures(), "4")).containsInAnyOrder("formatted bad");
        PAssert.that("Custom DoFn", toInputElements(customDoFn.failures(), "5")).containsInAnyOrder("formatted bad");
        PAssert.that("Origin", toInputElements(originFn.failures(), "6")).containsInAnyOrder("formatted bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenInputElementToStringFailingOrReturningNull_whenFailure_thenInputElementWithToString() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("bad"));

        // When.
        final Result<PCollection<String>, Failure> failingFunction = CollectionComposer.of(words)
                .withInputElementToString(ElementFormatAndEncodingTest::failingToString)
                .apply("Failing function", MapElementFn.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();
        final Result<PCollection<String>, Failure> nullFunction = CollectionComposer.of(words)
                .withInputElementToString(element -> null)
                .apply("Null function", MapElementFn.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();

        // Then.
        PAssert.that("Failing function", toInputElements(failingFunction.failures(), "1")).containsInAnyOrder("bad");
        PAssert.that("Null function", toInputElements(nullFunction.failures(), "2")).containsInAnyOrder("bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenNoOption_whenFailure_thenInputElementWithToStringAndNoEncodedElement() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("bad"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words)
                .apply(FAIL, MapElementFn.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();

        // Then.
        PAssert.that(result.failures().apply(MapElements.into(TypeDescriptors.strings())
                        .via((Failure failure) -> failure.getInputElement() + "|" + failure.getInputElementBytes() + "|" + failure.getInputElementCoder())))
                .containsInAnyOrder("bad|null|null");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenEncodedElements_whenFailuresInAllKindsOfSteps_thenInputElementsDecodedBackWithTheirCoder() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("bad"));
        final CollectionComposer<String> composer = CollectionComposer.of(words).withEncodedElements();

        // When.
        final Result<PCollection<String>, Failure> mapElements = composer
                .apply("MapElements", MapElements.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();
        final Result<PCollection<String>, Failure> mapElementFn = composer
                .apply("MapElementFn", MapElementFn.into(TypeDescriptors.strings()).via(ElementFormatAndEncodingTest::failIfBad))
                .getResult();
        final Result<PCollection<String>, Failure> filterFn = composer
                .apply("FilterFn", FilterFn.by(word -> !failIfBad(word).isEmpty()))
                .getResult();

        // Then.
        PAssert.that("MapElements", decodedInputElements(mapElements.failures(), StringUtf8Coder.of(), "1")).containsInAnyOrder("bad");
        PAssert.that("MapElementFn", decodedInputElements(mapElementFn.failures(), StringUtf8Coder.of(), "2")).containsInAnyOrder("bad");
        PAssert.that("FilterFn", decodedInputElements(filterFn.failures(), StringUtf8Coder.of(), "3")).containsInAnyOrder("bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenEncodedElementsOfKvs_whenFailure_thenKvDecodedBackWithTheKvCoder() {
        // Given.
        final PCollection<KV<String, Integer>> teams = pipeline.apply("Create teams", Create.of(KV.of("PSG", 0))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

        // When.
        final Result<PCollection<Integer>, Failure> result = CollectionComposer.of(teams)
                .withEncodedElements()
                .apply(FAIL, MapElementFn.into(TypeDescriptors.integers()).via((KV<String, Integer> team) -> 1 / team.getValue()))
                .getResult();

        // Then.
        PAssert.that(result.failures().apply(MapElements.into(TypeDescriptors.strings())
                        .via((Failure failure) -> decode(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), failure.getInputElementBytes()).toString())))
                .containsInAnyOrder(KV.of("PSG", 0).toString());

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenEncodedElementsOfAvroGenericRecords_whenFailure_thenRecordDecodedBackWithTheAvroCoder() {
        // Given.
        final AvroCoder<GenericRecord> avroCoder = AvroCoder.of(GenericRecord.class, AvroTest.getClassSchema());
        final PCollection<GenericRecord> records = pipeline.apply("Create records", Create.of(toGenericRecord("bad")).withCoder(avroCoder));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(records)
                .withEncodedElements()
                .apply(FAIL, MapElementFn.into(TypeDescriptors.strings()).via((GenericRecord record) -> failIfBad(record.get("name").toString())))
                .getResult();

        // Then.
        PAssert.that(result.failures().apply(MapElements.into(TypeDescriptors.strings())
                        .via((Failure failure) -> decode(AvroCoder.of(GenericRecord.class, AvroTest.getClassSchema()), failure.getInputElementBytes())
                                .get("name").toString())))
                .containsInAnyOrder("bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenEncodedElementsAndOrigin_whenFailure_thenValueAndOriginDecodedBackWithTheirCoders() {
        // Given.
        final PCollection<String> messages = pipeline.apply("Create messages", Create.of("psg,bad"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(messages)
                .withOriginElement(message -> "origin " + message)
                .withEncodedElements()
                .apply("To words", FlatMapElementFn.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split(","))))
                .apply("Validate", FilterFn.by(word -> !failIfBad(word).isEmpty()))
                .getResult();

        // Then.
        PAssert.that(result.failures().apply(MapElements.into(TypeDescriptors.strings())
                        .via((Failure failure) -> decode(StringUtf8Coder.of(), failure.getInputElementBytes())
                                + "|" + decode(StringUtf8Coder.of(), failure.getOriginElementBytes())
                                + "|" + failure.getOriginElement())))
                .containsInAnyOrder("bad|psg,bad|origin psg,bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenValueCoderSetWithSetCoderAndOrigin_whenFailure_thenValueAndOriginEncoded() {
        // Given: the output type of the first step has no coder inferred by Beam, it's set with setCoder.
        final PCollection<String> names = pipeline.apply("Create names", Create.of("bad"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(names)
                .withOriginElement(name -> name)
                .withEncodedElements()
                .apply("To record", MapElementFn.into(TypeDescriptor.of(GenericRecord.class)).via(ElementFormatAndEncodingTest::toGenericRecord))
                .setCoder(AvroCoder.of(GenericRecord.class, AvroTest.getClassSchema()))
                .apply("Validate", MapElementFn.into(TypeDescriptors.strings()).via((GenericRecord record) -> failIfBad(record.get("name").toString())))
                .getResult();

        // Then: the value coder is known from setCoder, the value and the origin are encoded.
        PAssert.that(result.failures().apply(MapElements.into(TypeDescriptors.strings())
                        .via((Failure failure) -> (failure.getInputElementBytes() != null) + "|" + decode(StringUtf8Coder.of(), failure.getOriginElementBytes()))))
                .containsInAnyOrder("true|bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenCoderFailingToEncode_whenEncodeInputElement_thenFailureUnchangedWithoutBytes() {
        // Given.
        final Failure failure = Failure.from(FAIL, "element", new IllegalStateException("Error"));

        // When.
        final Failure resultFailure = failure.withEncodedInputElement("element", new FailingCoder());

        // Then.
        assertThat(resultFailure).isSameAs(failure);
        assertThat(resultFailure.getInputElementBytes()).isNull();
        assertThat(resultFailure.getInputElementCoder()).isNull();
    }

    @Test
    public void givenEncodedInputElement_whenGetBytes_thenCopyOfTheBytes() throws Exception {
        // Given.
        final Failure failure = Failure.from(FAIL, "element", new IllegalStateException("Error"))
                .withEncodedInputElement("element", StringUtf8Coder.of());

        // When.
        failure.getInputElementBytes()[0] = 0;

        // Then.
        assertThat(CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), failure.getInputElementBytes())).isEqualTo("element");
        assertThat(failure.getInputElementCoder()).isEqualTo(StringUtf8Coder.of().toString());
    }

    private static String failIfBad(final String word) {
        if (word.equals("bad")) {
            throw new IllegalArgumentException("Bad word");
        }
        return word;
    }

    private static String failingToString(final Object element) {
        throw new IllegalStateException("Format error");
    }

    private static GenericRecord toGenericRecord(final String name) {
        final GenericRecord record = new GenericData.Record(AvroTest.getClassSchema());
        record.put("id", 1);
        record.put("name", name);
        return record;
    }

    private static <T> T decode(final Coder<T> coder, final byte[] bytes) {
        try {
            return CoderUtils.decodeFromByteArray(coder, bytes);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static PCollection<String> toInputElements(final PCollection<Failure> failures, final String suffix) {
        return failures.apply("To input elements " + suffix, MapElements.into(TypeDescriptors.strings()).via(Failure::getInputElement));
    }

    private static PCollection<String> decodedInputElements(final PCollection<Failure> failures,
                                                            final Coder<String> coder,
                                                            final String suffix) {
        return failures.apply("Decode input elements " + suffix, MapElements.into(TypeDescriptors.strings())
                .via((Failure failure) -> decode(coder, failure.getInputElementBytes())));
    }

    /**
     * Custom DoFn with the default constructor, calling outputFailure.
     */
    private static class FailIfBadFn extends BaseElementFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            try {
                ctx.output(failIfBad(ctx.element()));
            } catch (Throwable throwable) {
                outputFailure(ctx, throwable);
            }
        }
    }

    /**
     * Coder failing to encode.
     */
    private static class FailingCoder extends AtomicCoder<String> {
        @Override
        public void encode(final String value, final OutputStream outStream) {
            throw new IllegalStateException("Encoding error");
        }

        @Override
        public String decode(final InputStream inStream) {
            throw new UnsupportedOperationException();
        }
    }
}
