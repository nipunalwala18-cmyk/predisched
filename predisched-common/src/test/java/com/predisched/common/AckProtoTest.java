package com.predisched.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.Ack;
import org.junit.jupiter.api.Test;

public class AckProtoTest {

    @Test
    public void buildsAckProto() {
        Ack ack = Ack.newBuilder().setOk(true).setMessage("ok").build();
        assertTrue(ack.getOk());
        assertEquals("ok", ack.getMessage());
    }
}
