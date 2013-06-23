/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public enum ResponseCode {
    SUCCESS, 
    BUSY, 
    LOOKUP_TIMEOUT, 
    READ_TIMEOUT, 
    WRITE_TIMEOUT, 
    CLIENT_TIMEOUT, 
    RANGEQUERY_TIMEOUT, 
    SUCCESS_INTERRUPTED,
    UNSUPPORTED_OP;
}
