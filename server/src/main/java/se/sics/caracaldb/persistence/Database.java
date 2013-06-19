/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.persistence;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Database extends Persistence {
    /**
     * Closes the database in an orderly fashion.
     * 
     * Should be idempotent, as it might be called multiple times.
     */
    public void close();
}
