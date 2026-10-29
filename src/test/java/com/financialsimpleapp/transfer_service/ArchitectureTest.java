package com.financialsimpleapp.transfer_service;

import static com.tngtech.archunit.base.DescribedPredicate.alwaysTrue;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.onionArchitecture;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.List;

/**
 * Enforces the hexagonal (ports and adapters) boundaries: adapters -> application ports -> domain,
 * never the other way round.
 *
 * <p>The {@code config} package is the composition root and is free to wire everything together.
 */
@AnalyzeClasses(
    packages = "com.financialsimpleapp.transfer_service",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  @ArchTest
  static final ArchRule layers_point_inwards =
      onionArchitecture()
          .domainModels("..transfer.domain..")
          .applicationServices("..transfer.application..")
          .adapter("web", "..transfer.adapter.in.web..")
          .adapter("inbound-messaging", "..transfer.adapter.in.messaging..")
          .adapter("outbound-messaging", "..transfer.adapter.out.messaging..")
          .adapter("persistence", "..transfer.adapter.out.persistence..")
          .withOptionalLayers(true)
          .ignoreDependency(resideInAPackage("..transfer_service.config.."), alwaysTrue());

  @ArchTest
  static final ArchRule domain_depends_only_on_the_jdk =
      classes()
          .that()
          .resideInAPackage("..transfer.domain..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage("..transfer.domain..", "java..")
          .because("the domain must stay free of frameworks and infrastructure");

  @ArchTest
  static final ArchRule adapters_use_ports_not_services =
      noClasses()
          .that()
          .resideInAPackage("..transfer.adapter..")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("..transfer.application.service..")
          .because("adapters must talk to the application through its ports");

  @ArchTest
  static final ArchRule ports_do_not_depend_on_services =
      noClasses()
          .that()
          .resideInAPackage("..transfer.application.port..")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("..transfer.application.service..");

  @ArchTest
  static final ArchRule application_is_free_of_technology_details =
      noClasses()
          .that()
          .resideInAPackage("..transfer.application..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework.web..",
              "org.springframework.kafka..",
              "org.springframework.jdbc..",
              "org.apache.kafka..",
              "jakarta.servlet..",
              "java.sql..",
              "com.fasterxml.jackson..",
              "tools.jackson..")
          .because("technology details belong in adapters");

  @ArchTest
  static final ArchRule output_ports_are_implemented_by_outbound_adapters =
      classes()
          .that()
          .implement(resideInAPackage("..transfer.application.port.out.."))
          .and()
          .areNotInterfaces()
          .should()
          .resideInAPackage("..transfer.adapter.out..");

  @ArchTest
  static final ArchRule input_ports_are_implemented_by_services =
      classes()
          .that()
          .implement(resideInAPackage("..transfer.application.port.in.."))
          .and()
          .areNotInterfaces()
          .should()
          .resideInAPackage("..transfer.application.service..");

  @ArchTest
  static final ArchRule input_ports_are_implemented_one_per_service_named_after_the_port =
      classes()
          .that()
          .implement(resideInAPackage("..transfer.application.port.in.."))
          .and()
          .areNotInterfaces()
          .should(implementExactlyOneInputPortAndBeNamedAfterIt())
          .because(
              "FooUseCase is implemented by FooService, so each is easy to find from the other");

  private static ArchCondition<JavaClass> implementExactlyOneInputPortAndBeNamedAfterIt() {
    return new ArchCondition<>("implement exactly one input port and be named <Port>Service") {
      @Override
      public void check(JavaClass service, ConditionEvents events) {
        List<String> ports =
            service.getRawInterfaces().stream()
                .filter(port -> port.getPackageName().contains(".application.port.in"))
                .map(JavaClass::getSimpleName)
                .toList();

        if (ports.size() != 1) {
          events.add(
              SimpleConditionEvent.violated(
                  service, service.getName() + " implements " + ports.size() + " input ports"));
          return;
        }

        String expected = ports.getFirst().replaceFirst("UseCase$", "") + "Service";
        if (!service.getSimpleName().equals(expected)) {
          events.add(
              SimpleConditionEvent.violated(
                  service,
                  service.getName()
                      + " implements "
                      + ports.getFirst()
                      + " and should be named "
                      + expected));
        }
      }
    };
  }
}
