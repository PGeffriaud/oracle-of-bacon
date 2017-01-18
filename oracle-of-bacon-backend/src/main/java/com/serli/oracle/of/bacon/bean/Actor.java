package com.serli.oracle.of.bacon.bean;

import io.searchbox.annotations.JestId;

/**
 * Created by PierreG on 18/01/17.
 *
 */
public class Actor {

    @JestId
    private String name;

    public Actor(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
