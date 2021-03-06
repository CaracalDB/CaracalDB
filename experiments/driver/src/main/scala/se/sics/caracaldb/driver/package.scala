package se.sics.caracaldb

import se.sics.kompics.Component
import se.sics.kompics.ComponentProxy

package object driver {
    type ComponentSetup = ComponentProxy => Component

    case object Status
    case object Ready
}