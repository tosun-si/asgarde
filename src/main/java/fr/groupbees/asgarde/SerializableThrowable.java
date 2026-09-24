package fr.groupbees.asgarde;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Serializable stand-in for an exception that can't be serialized (e.g. an exception holding a client or a lock).
 *
 * <p>
 * A {@link Failure} is encoded with Java serialization: storing a non serializable exception as is would make
 * the job fail, which is exactly what the error handling must avoid. This class keeps the original class name,
 * message, stack trace, causes and suppressed exceptions.
 * </p>
 *
 * <p>
 * Serializable exceptions are kept as is by {@link #of(Throwable)}.
 * </p>
 */
public final class SerializableThrowable extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String originalClassName;

    private SerializableThrowable(final Throwable original) {
        super(original.getMessage());
        this.originalClassName = original.getClass().getName();
        setStackTrace(original.getStackTrace());
    }

    /**
     * Returns the given exception if it's serializable, otherwise a {@link SerializableThrowable} copy of it.
     *
     * @param throwable the exception to check
     * @return a serializable exception
     */
    public static Throwable of(final Throwable throwable) {
        if (throwable == null || isSerializable(throwable)) {
            return throwable;
        }

        final SerializableThrowable copy = new SerializableThrowable(throwable);

        if (throwable.getCause() != null && throwable.getCause() != throwable) {
            copy.initCause(of(throwable.getCause()));
        }
        for (Throwable suppressed : throwable.getSuppressed()) {
            copy.addSuppressed(of(suppressed));
        }

        return copy;
    }

    /**
     * @return the class name of the original exception
     */
    public String getOriginalClassName() {
        return originalClassName;
    }

    @Override
    public String toString() {
        final String message = getLocalizedMessage();
        return message != null ? originalClassName + ": " + message : originalClassName;
    }

    private static boolean isSerializable(final Throwable throwable) {
        try (ObjectOutputStream out = new ObjectOutputStream(NullOutputStream.INSTANCE)) {
            out.writeObject(throwable);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static final class NullOutputStream extends OutputStream {
        private static final NullOutputStream INSTANCE = new NullOutputStream();

        @Override
        public void write(final int b) {
        }

        @Override
        public void write(final byte[] b, final int off, final int len) {
        }
    }
}
