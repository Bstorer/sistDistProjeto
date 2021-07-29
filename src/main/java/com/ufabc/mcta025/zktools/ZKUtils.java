package com.ufabc.mcta025.zktools;

public class ZKUtils {
    public static String intToSequence(int sequence) {
        return String.format("%010d", sequence);
    }

    public static int sequenceToInt(String sequence) {
        return new Integer(sequence.substring(sequence.length() - 10, sequence.length()));
    }
}
