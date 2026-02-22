package com.priyan.router.internal;

import org.mule.sdk.api.annotation.Extension;
import org.mule.sdk.api.annotation.Configurations;
import org.mule.sdk.api.annotation.dsl.xml.Xml;

@Xml(prefix = "custom-aggregator")
@Extension(name = "Custom-aggregator")
@Configurations(CustomaggregatorConfiguration.class)
public class CustomaggregatorExtension {

}
